using CoreLibrary.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.OpenApi.Validations;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace CreateAndLoadDynamoDBTables.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class LoadDataToDatabaseController : ControllerBase
    {
        //GET: api/<CreateTableAndLoadDataController>
        [HttpGet("{id}")]
        [Tags("LoadDataToDatabaseFromStreamReader")]
        public async Task GetComments(int id = 0)
        {
            string commentsPath = @"I:\IT\youtube\zeroCarbLifes\zeroCarbLifeComments.txt";

            System.IO.File.ReadAllText(commentsPath);
            using var reader = new StreamReader(commentsPath);
            var currentLine = new StringBuilder();
            var title = new StringBuilder();
            var comment = new StringBuilder();
            var guid = new Guid();
            var comments = new List<Comment>();
            var titles = new Dictionary<Guid, string>();
            var sw = new Stopwatch();
            sw.Start();

            while ((currentLine.Append(await reader.ReadLineAsync()) != null))
            {
                if (string.IsNullOrEmpty(currentLine.ToString())) continue;

                if (currentLine.ToString().StartsWith("***"))
                {
                    title.Clear();
                    guid = Guid.NewGuid();
                    title.Append(currentLine.ToString());
                    titles[guid] = currentLine.ToString();
                }

                if (currentLine.ToString().StartsWith('@') && comment.ToString().StartsWith('@'))
                {
                    var commentEntry = new Comment() { Guid = guid, Text = comment.ToString() };
                    comments.Add(commentEntry);
                    comment.Clear();
                }


                if (!string.IsNullOrEmpty(currentLine.ToString()) && !currentLine.ToString().StartsWith("****"))
                {
                    if (currentLine.ToString().StartsWith('@'))
                        comment.Append(currentLine.ToString() + Environment.NewLine);
                    else
                        comment.Append(currentLine.ToString() + " ");
                }

                if (currentLine.ToString().Contains("Total Number Of Comments"))
                    break;

                currentLine.Clear();
            }

            sw.Stop();
            var elapsed = sw.Elapsed;
        }

        [HttpGet]
        [Tags("LoadDataToDatabaseFromFileReaderRegex")]
        public async Task LoadCommentsToDatabase()
        {
            string commentsPath = @"I:\IT\youtube\zeroCarbLifes\zeroCarbLifeComments.txt";

            var comments = System.IO.File.ReadAllText(commentsPath);

            var pattern = "@";
            var regex = new Regex(pattern);

            var matches = regex.Matches(comments);
        }
    }
}
