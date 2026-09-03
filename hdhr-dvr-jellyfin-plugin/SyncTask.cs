namespace Com.ZachDeibert.MediaTools.Hdhr.Dvr.Jellyfin;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Com.ZachDeibert.MediaTools.Hdhr.Api;
using Com.ZachDeibert.MediaTools.Hdhr.Dvr.Core;
using Com.ZachDeibert.MediaTools.Hdhr.Dvr.Core.Configuration;
using Com.ZachDeibert.MediaTools.Hdhr.Dvr.Core.Metadata;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Common.Net;
using MediaBrowser.Model.LiveTv;
using MediaBrowser.Model.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

public class SyncTask(IConfigurationManager config, IHttpClientFactory httpClientFactory, ILogger<DownloadJob> downloadLogger, ILogger<SyncTask> logger) : IScheduledTask {
    public string Category => "Live TV";

    public string Description => "Downloads new recordings from connected HDHomeRun DVRs";

    public string Key => GetType().FullName!;

    public string Name => "Sync HDHomeRun DVR";

    public async Task ExecuteAsync(IProgress<double> progress, CancellationToken cancellationToken) {
        Dictionary<RecordingCategory, Context> libraries = new[] {
            (RecordingCategory.Movie, Plugin.Instance?.Configuration.MovieRecordingPath),
            (RecordingCategory.Series, Plugin.Instance?.Configuration.SeriesRecordingPath)
        }.Where(l => !string.IsNullOrWhiteSpace(l.Item2)).ToDictionary(l => l.Item1, l => new Context() { DbPath = Path.Join(l.Item2!, $"{typeof(Plugin).Namespace}.db") });
        foreach (Context library in libraries.Values) {
            await library.Database.MigrateAsync(cancellationToken);
        }
        using HttpClient httpClient = httpClientFactory.CreateClient(NamedClient.Default);
        DownloadManager downloadManager = new(httpClient, downloadLogger);
        foreach (TunerHostInfo tunerHost in config.GetConfiguration<LiveTvOptions>("livetv").TunerHosts) {
            Tuner? tuner = await new TunerRef(tunerHost.Url).Discover(httpClient, logger, cancellationToken);
            if (tuner == null) {
                continue;
            }
            if (tuner.Storage == null) {
                logger.LogInformation("Skipping tuner {Url} which has no DVR storage", tuner.BaseUrl);
                continue;
            }
            HashSet<string> storagesScanned = [];
            foreach (RecordingStorage storage in await tuner.Storage.GetRecordings(httpClient, logger, cancellationToken)) {
                if (!storagesScanned.Add(storage.Url)) {
                    continue;
                }
                Dictionary<RecordingCategory, Series> categorySeries = [];
                foreach (Recording recording in await storage.GetRecordings(httpClient, logger, cancellationToken)) {
                    if (!libraries.TryGetValue(recording.Category, out Context? library)) {
                        logger.LogError("Series {SeriesName} has unknown type {Category}", storage.Title, storage.Category);
                        continue;
                    } else if (recording.RecordEndTime + TimeSpan.FromSeconds(30) > DateTimeOffset.UtcNow) {
                        logger.LogInformation("Not downloading {SeriesName} episode {EpisodeNumber} because it is still recording", storage.Title, recording.EpisodeNumber);
                        continue;
                    }
                    if (!categorySeries.TryGetValue(recording.Category, out Series? series)) {
                        series = await library.Series.FirstOrDefaultAsync(
                            s => s.Metadata!.SeriesId == storage.SeriesId
                            && s.Metadata!.Title == storage.Title
                            && s.Metadata!.Category == storage.Category
                            && s.Metadata!.ImageUrl == storage.ImageUrl
                            && s.Metadata!.PosterUrl == storage.PosterUrl
                            && s.Metadata!.IsNew == storage.IsNew
                            && s.Metadata!.Url == storage.Url,
                            cancellationToken
                        ) ?? (await library.Series.AddAsync(new() {
                            Metadata = storage,
                        }, cancellationToken)).Entity;
                        categorySeries.Add(recording.Category, series);
                    }
                    Episode? episode = await library.Episodes.FirstOrDefaultAsync(e => e.Series == series && e.Metadata!.Filename == recording.Filename && e.DeleteReason == DeleteReason.NotDeleted, cancellationToken);
                    if (episode?.DownloadInterrupted == false) {
                        logger.LogInformation("Skipping download of {SeriesName} episode {EpisodeNumber} because it already exists", storage.Title, recording.EpisodeNumber);
                        continue;
                    }
                    Episode newEpisode = new() {
                        Series = series,
                        SeriesStartTime = storage.StartTime,
                        Metadata = recording,
                        DownloadInterrupted = true,
                        DownloadStarted = DateTime.Now,
                        DownloadReason = episode == null ? DownloadReason.New : DownloadReason.DownloadInterrupted,
                        DeleteReason = DeleteReason.NotDeleted,
                        ReRecordable = false,
                    };
                    string? target = newEpisode.FilePath();
                    if (target == null) {
                        logger.LogWarning("Series {SeriesName} has unconfigured type {Category}", storage.Title, storage.Category);
                        continue;
                    }
                    if (episode == null) {
                        if (File.Exists(target)) {
                            logger.LogWarning("Skipping download of {SeriesName} episode {EpisodeNumber} because it already exists at {Path}", storage.Title, recording.EpisodeNumber, target);
                            continue;
                        }
                        logger.LogInformation("Found {SeriesName} episode {EpisodeNumber} to download from {Url}", storage.Title, recording.EpisodeNumber, recording.PlayUrl);
                    } else {
                        episode.DeleteReason = DeleteReason.ReDownloaded;
                        logger.LogInformation("Retrying download of {SeriesName} episode {EpisodeNumber} from {Url}", storage.Title, recording.EpisodeNumber, recording.PlayUrl);
                    }
                    await downloadManager.Add(new() {
                        Db = library,
                        Episode = (await library.Episodes.AddAsync(newEpisode, cancellationToken)).Entity,
                    }, cancellationToken);
                }
            }
        }
        foreach (Context library in libraries.Values) {
            _ = await library.SaveChangesAsync(cancellationToken);
        }
        await downloadManager.Run(progress, cancellationToken);
        if (!cancellationToken.IsCancellationRequested) {
            foreach (Context library in libraries.Values) {
                IQueryable<Episode> allEpisodes = library.Episodes.Where(e => e.DownloadInterrupted == false && e.DeleteReason == DeleteReason.NotDeleted).Include(e => e.Series);
                IEnumerable<Episode>? delete = null;
                switch (Plugin.Instance?.Configuration.DeletePolicy) {
                    case DeletePolicy.AfterDeleted:
                        delete = allEpisodes.AsEnumerable().Where(e => !File.Exists(e.FilePath(false)));
                        break;

                    case DeletePolicy.AfterOneWeek:
                    case DeletePolicy.AfterOneDay:
                        DateTime threshold = DateTime.Now - Plugin.Instance.Configuration.DeletePolicy switch {
                            DeletePolicy.AfterOneWeek => TimeSpan.FromDays(7),
                            DeletePolicy.AfterOneDay => TimeSpan.FromDays(1),
                            _ => throw new ArgumentException(nameof(Plugin.Instance.Configuration.DeletePolicy)),
                        };
                        delete = allEpisodes.Where(e => e.DownloadStarted <= threshold);
                        break;

                    case DeletePolicy.AfterDownload:
                        delete = allEpisodes;
                        break;
                }
                if (delete != null) {
                    foreach (Episode episode in delete) {
                        await new DownloadJob() {
                            Db = library,
                            Episode = episode,
                        }.Delete(Plugin.Instance?.Configuration.DeletePolicy switch {
                            DeletePolicy.AfterDeleted => DeleteReason.Deleted,
                            DeletePolicy.AfterOneWeek => DeleteReason.OneWeekPassed,
                            DeletePolicy.AfterOneDay => DeleteReason.OneDayPassed,
                            DeletePolicy.AfterDownload => DeleteReason.Downloaded,
                            _ => throw new ArgumentException(nameof(Plugin.Instance.Configuration.DeletePolicy)),
                        }, httpClient, downloadLogger, cancellationToken);
                    }
                }
                foreach (Series series in library.Series) {
                    string? seriesDir = series.FilePath(false);
                    if (seriesDir != null && Directory.Exists(seriesDir)) {
                        string thumbnailPath = Path.Join(seriesDir, "thumbnail" + Path.GetExtension(series.Metadata!.ImageUrl));
                        if (File.Exists(thumbnailPath)) {
                            logger.LogTrace("Thumbnail file {Path} already exists", thumbnailPath);
                        } else {
                            logger.LogInformation("Downloading {Path} from {Url}", thumbnailPath, series.Metadata.ImageUrl);
                            using HttpResponseMessage response = await httpClient.GetAsync(series.Metadata.ImageUrl, HttpCompletionOption.ResponseContentRead, cancellationToken);
                            await File.WriteAllBytesAsync(thumbnailPath, await response.EnsureSuccessStatusCode().Content.ReadAsByteArrayAsync(cancellationToken), cancellationToken);
                        }
                        if (series.Metadata.PosterUrl != null) {
                            string coverPath = Path.Join(seriesDir, "cover" + Path.GetExtension(series.Metadata.PosterUrl));
                            if (File.Exists(coverPath)) {
                                logger.LogTrace("Cover file {Path} already exists", coverPath);
                            } else {
                                logger.LogInformation("Downloading {Path} from {Url}", coverPath, series.Metadata.PosterUrl);
                                using HttpResponseMessage response = await httpClient.GetAsync(series.Metadata.PosterUrl, HttpCompletionOption.ResponseContentRead, cancellationToken);
                                await File.WriteAllBytesAsync(coverPath, await response.EnsureSuccessStatusCode().Content.ReadAsByteArrayAsync(cancellationToken), cancellationToken);
                            }
                        }
                    }
                }
            }
            if (libraries.TryGetValue(RecordingCategory.Movie, out Context? moviesDb)) {
                XmlSerializer movieXml = new(typeof(Movie));
                foreach (Episode episode in moviesDb.Episodes.Where(e => !e.DownloadInterrupted).Include(e => e.Series)) {
                    string? episodePath = episode.FilePath(false);
                    string? nfoPath = Path.ChangeExtension(episodePath, "nfo");
                    if (episodePath != null && File.Exists(episodePath) && nfoPath != null && !File.Exists(nfoPath)) {
                        logger.LogInformation("Creating NFO file {Path}", nfoPath);
                        using StreamWriter stream = File.CreateText(nfoPath);
                        movieXml.Serialize(stream, new Movie(episode));
                    }
                }
            }
            if (libraries.TryGetValue(RecordingCategory.Series, out Context? seriesDb)) {
                XmlSerializer episodeDetailsXml = new(typeof(EpisodeDetails));
                XmlSerializer seasonXml = new(typeof(Season));
                XmlSerializer tvShowXml = new(typeof(TvShow));
                foreach (Episode episode in seriesDb.Episodes.Where(e => !e.DownloadInterrupted).Include(e => e.Series)) {
                    string? episodePath = episode.FilePath(false);
                    if (episodePath != null && File.Exists(episodePath)) {
                        string? episodeNfoPath = Path.ChangeExtension(episodePath, "nfo");
                        string? seasonNfoPath = episode.Metadata!.EpisodeNumber == null ? null : Path.Join(Path.GetDirectoryName(episodeNfoPath), "season.nfo");
                        string? tvShowNfoPath = Path.Join(Path.GetDirectoryName(Path.GetDirectoryName(seasonNfoPath) ?? episodeNfoPath), "tvshow.nfo");
                        if (tvShowNfoPath != null && !File.Exists(tvShowNfoPath)) {
                            logger.LogInformation("Creating NFO file {Path}", tvShowNfoPath);
                            using StreamWriter stream = File.CreateText(tvShowNfoPath);
                            tvShowXml.Serialize(stream, new TvShow(episode.Series!));
                        }
                        if (seasonNfoPath != null && !File.Exists(seasonNfoPath)) {
                            logger.LogInformation("Creating NFO file {Path}", seasonNfoPath);
                            using StreamWriter stream = File.CreateText(seasonNfoPath);
                            seasonXml.Serialize(stream, new Season(new EpisodeNumber(episode.Metadata.EpisodeNumber!).Season));
                        }
                        if (episodeNfoPath != null && !File.Exists(episodeNfoPath)) {
                            logger.LogInformation("Creating NFO file {Path}", episodeNfoPath);
                            using StreamWriter stream = File.CreateText(episodeNfoPath);
                            episodeDetailsXml.Serialize(stream, new EpisodeDetails(episode));
                        }
                    }
                }
            }
        }
    }

    public IEnumerable<TaskTriggerInfo> GetDefaultTriggers() => [
        new TaskTriggerInfo {
            Type = TaskTriggerInfoType.StartupTrigger,
        },
        new TaskTriggerInfo {
            IntervalTicks = TimeSpan.FromMinutes(10).Ticks,
            Type = TaskTriggerInfoType.IntervalTrigger,
        },
    ];
}
