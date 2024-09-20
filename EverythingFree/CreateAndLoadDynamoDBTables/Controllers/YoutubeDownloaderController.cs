using DataLayer.Interfaces;
using Microsoft.AspNetCore.Mvc;
using YoutubeDLSharp;
using YoutubeDLSharp.Options;
using YoutubeDLSharp.Metadata;
using System.Text;
using Swashbuckle.AspNetCore.SwaggerGen;
using System.Diagnostics;
using System.Data.SqlTypes;
using System.Text.RegularExpressions;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace CreateAndLoadDynamoDBTables.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class YoutubeDownloaderController : ControllerBase
    {
        private readonly ICreateTablesLoadData _createTableAndLoadData;

        public YoutubeDownloaderController(ICreateTablesLoadData createTablesLoadData)
        {
            _createTableAndLoadData = createTablesLoadData;
        }

        //GET: api/<CreateTableAndLoadDataController>
        [HttpGet("{id}")]
        [Tags("GetCommentsToSumRegex")]
        public IEnumerable<string> GetComments(int id = 0)
        {
            string pathOfComments = @"I:\IT\youtube\KenDBerry\eddigi\KenDBerryComments full.txt";
            var commentsAll = System.IO.File.ReadAllLines(pathOfComments).ToList();
            int numberOfComments = 0;

            var filteredComments = commentsAll.Where(x => x.Contains("- Comments:")).ToList();

            string pattern = @"- Comments: (\d*)";
            Regex regex = new(pattern);

            filteredComments.ForEach(x => numberOfComments += int.Parse(regex.Match(x).Groups[1].Value));    

            return new List<string> { pathOfComments };

        }

        // GET api/<CreateTableAndLoadDataController>/5
        [HttpGet/*("{id}")*/]
        [Tags("YoutubeComments")]
        public async Task<string> Get(/*int id*/)
        {
            string nameOfTheYoutubeChannel = "teszt";
            string pathOfTheVideoLinks = @$"I:\IT\youtube\{nameOfTheYoutubeChannel}\{nameOfTheYoutubeChannel}VideoLinks.txt";
            string pathOfCommentsToBeSaved = @$"I:\IT\youtube\{nameOfTheYoutubeChannel}\{nameOfTheYoutubeChannel}Comments.txt";
            string pathLogFile = @$"I:\IT\youtube\{nameOfTheYoutubeChannel}\{nameOfTheYoutubeChannel}Log.txt";
            
            var linksOfVideos = System.IO.File.ReadAllLines(pathOfTheVideoLinks);
            int numberOfVideos = 0;
            long? numberOfComments = 0;

            StringBuilder sb = new StringBuilder();
            sb.AppendLine().AppendLine().AppendLine();

            var ytdl = new YoutubeDL();
            ytdl.YoutubeDLPath = @"I:\IT\youtube\yt-dlp.exe";

            var options = new OptionSet()
            {
                WriteComments = true
            };

            Stopwatch sw = Stopwatch.StartNew();

            try
            {
                foreach (var linkOfVideo in linksOfVideos)
                {
                    numberOfVideos++;
                    //ytdl.FFmpegPath = "path\\to\\ffmpeg.exe";
                    var res = await ytdl.RunVideoDataFetch("https://www.youtube.com/watch?v=KVVyyvWNUlQ&t=1s", overrideOptions: options);
                    // get some video information
                    VideoData video = res.Data;
                    if (video is null || video.Comments is null)
                    {
                        System.IO.File.AppendAllText(pathLogFile, $"video is null or comment is null. video number: {numberOfVideos.ToString()} video link: {linkOfVideo}{Environment.NewLine}");
                        continue;
                    }
                    sb.Append($"******   {video.Title} - {video.UploadDate?.ToString("yyyy-MM-dd")} - Comments: {video.CommentCount} - video number: {numberOfVideos} - elapsed time: {sw.Elapsed.Hours}:{sw.Elapsed.Minutes}:{sw.Elapsed.Seconds}   ******").AppendLine().AppendLine();
                    sb.AppendJoin(Environment.NewLine, video.Comments.Select(x => x.Author + " - " +  x.Timestamp.Date + Environment.NewLine + x.Text + Environment.NewLine).ToArray())
                    .AppendLine().AppendLine().AppendLine();

                    System.IO.File.AppendAllText(pathOfCommentsToBeSaved, sb.ToString());
                    sb.Clear();
                    numberOfComments += video.CommentCount is null ? 0 : video.CommentCount;
                    if (numberOfVideos == 5000) break;
                }

            }
            catch (Exception ex)
            {
                sb.Clear();
                sb.AppendLine(ex.Message)
                .AppendLine(ex.StackTrace)
                .AppendLine($"videonumber: {numberOfVideos}");
                System.IO.File.AppendAllText(pathLogFile, sb.ToString());
            }

            sb.AppendLine($"Total Number Of Comments: {numberOfComments}");
            sb.Append($"Total Time Taken: {sw.Elapsed}");

            System.IO.File.AppendAllText(pathOfCommentsToBeSaved, sb.ToString());

            sw.Stop();
            return sw.Elapsed.ToString();
        }

        // POST api/<CreateTableAndLoadDataController>
        [HttpPost]
        public async Task Post()
        {
            await _createTableAndLoadData.CreateTableAndLoadData();
        }
    }
}
