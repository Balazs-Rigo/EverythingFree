using DataLayer.Interfaces;
using Microsoft.AspNetCore.Mvc;
using YoutubeDLSharp;
using YoutubeDLSharp.Options;
using YoutubeDLSharp.Metadata;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace CreateAndLoadDynamoDBTables.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CreateTableAndLoadDataController : ControllerBase
    {
        private readonly ICreateTablesLoadData _createTableAndLoadData;

        public CreateTableAndLoadDataController(ICreateTablesLoadData createTablesLoadData)
        {
            _createTableAndLoadData = createTablesLoadData;
        }

        // GET: api/<CreateTableAndLoadDataController>
        [HttpGet]
        public IEnumerable<string> Get()
        {
          
            return new string[] { "value1", "value2" };
        }

        // GET api/<CreateTableAndLoadDataController>/5
        [HttpGet("{id}")]
        [Tags("YoutubeComments")]
        public async Task<string> Get(int id)
        {
            var ytdl = new YoutubeDL();
            ytdl.YoutubeDLPath = @"I:\IT\youtube\yt-dlp.exe";

            var options = new OptionSet()
            {
                WriteComments = true
            };

            //ytdl.FFmpegPath = "path\\to\\ffmpeg.exe";
            var res = await ytdl.RunVideoDataFetch("https://www.youtube.com/watch?v=Zp7lpLC-kxs&t=103s", overrideOptions: options);
            // get some video information
            VideoData video = res.Data;
            var comments = video.Comments.Select(x => x.Text).ToList();           
           

            return "value";
        }

        // POST api/<CreateTableAndLoadDataController>
        [HttpPost]
        public async Task Post()
        {
            await _createTableAndLoadData.CreateTableAndLoadData();
        }

        // PUT api/<CreateTableAndLoadDataController>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/<CreateTableAndLoadDataController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }   
    }
}
