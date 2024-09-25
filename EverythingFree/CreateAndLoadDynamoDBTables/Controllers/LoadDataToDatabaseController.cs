using CoreLibrary.Models;
using DataLayer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Routing;
using Microsoft.Data.SqlClient;
using Microsoft.OpenApi.Validations;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.RegularExpressions;

namespace CreateAndLoadDynamoDBTables.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class LoadDataToDatabaseController : ControllerBase
    {
        private readonly PostgresDBContext _dbContext;

        public LoadDataToDatabaseController(PostgresDBContext dbContext)
        {
                _dbContext = dbContext;
        }

        //GET: api/<CreateTableAndLoadDataController>
        [HttpPost("{id}")]
        [Tags("LoadDataToDatabaseFromStreamReaderBulkInsert")]
        public async Task GetComments(int id = 0)
        {
            string pathOfDirectory = @"I:\IT\youtube\!!!ALL";
            var files = Directory.GetFiles(pathOfDirectory, "*.*", SearchOption.TopDirectoryOnly);
            string commentsPath = @"I:\IT\youtube\LauraSpath\LauraSpathComments.txt";

            var CS = "Data Source=.;Initial Catalog=Youtube; Integrated Security=True;Trust Server Certificate=True";
           
            //System.IO.File.ReadAllText(commentsPath);

            DataTable commentsDataTable = new DataTable("Comments");
            DataTable videosDataTable = new DataTable("Videos");

            DataColumn idDataColumnComment = new("Id",typeof(Guid));
            DataColumn idDataColumnVideo = new("Id", typeof(Guid));
            commentsDataTable.Columns.Add(idDataColumnComment);
            videosDataTable.Columns.Add(idDataColumnVideo);

            DataColumn commentDataColumn = new("Comment");
            DataColumn videoDataColumn = new("Video");
            commentsDataTable.Columns.Add(commentDataColumn);
            videosDataTable.Columns.Add(videoDataColumn);

            using SqlConnection conn = new SqlConnection(CS);
           
            using SqlBulkCopy sqlBulkCopyComment = new SqlBulkCopy(conn);
            using SqlBulkCopy sqlBulkCopyVideo = new SqlBulkCopy(conn);

            sqlBulkCopyComment.DestinationTableName = "dbo.Comments";
            sqlBulkCopyComment.ColumnMappings.Add("Id", "Id");
            sqlBulkCopyComment.ColumnMappings.Add("Comment", "Comment");

            sqlBulkCopyVideo.DestinationTableName = "dbo.Videos";
            sqlBulkCopyVideo.ColumnMappings.Add("Id", "Id");
            sqlBulkCopyVideo.ColumnMappings.Add("Video", "Video");

            var currentLine = new StringBuilder();
            var title = new StringBuilder();
            var comment = new StringBuilder();
            var guid = new Guid();
            var comments = new List<Comments>();
            var titles = new Dictionary<Guid, string>();
            string filename = string.Empty;
            int lineCounter = 0;
            string currentLineString = string.Empty;
            string? line = string.Empty;

            var sw = new Stopwatch();
            sw.Start();

            foreach (var file in files)
            {
                if (Path.GetFileName(file).StartsWith('!')) continue;

                using (StreamReader reader = new(file))
                {
                    while (true)
                    {
                        line = await reader.ReadLineAsync();

                        if (line == null)
                        {
                            var commentEntry = new Comments() { Id = Guid.NewGuid(), Comment = comment.ToString() };
                            comments.Add(commentEntry);
                            commentsDataTable.Rows.Add(guid.ToString(), comment.ToString());

                            comment.Clear();
                            currentLine.Clear();
                            break;
                        }

                        currentLine.AppendLine(line);                       

                        if (string.IsNullOrEmpty(currentLine.ToString().Trim())) continue;

                        lineCounter = currentLine.ToString().Trim().StartsWith('@') ? ++lineCounter : 0;                        

                        if (currentLine.ToString().Contains("******   "))
                        {
                            title.Clear();
                            guid = Guid.NewGuid();
                            title.Append(currentLine.ToString().Substring(currentLine.ToString().IndexOf("******   ")));
                            titles[guid] = title.ToString();
                            videosDataTable.Rows.Add(guid.ToString(), title.ToString());
                        }                                              

                        var isCurrLine = currentLine.ToString().Trim().StartsWith("@");
                        var isComment = comment.ToString().Trim().StartsWith("@");

                        if (currentLine.ToString().Trim().StartsWith("@") && comment.ToString().Trim().StartsWith("@") && lineCounter != 2)
                        {
                            var commentEntry = new Comments() { Id = Guid.NewGuid(), Comment = comment.ToString() };
                            comments.Add(commentEntry);
                            commentsDataTable.Rows.Add(guid.ToString(), comment.ToString());

                            comment.Clear();
                        }

                        if ((!string.IsNullOrEmpty(currentLine.ToString()) || !string.IsNullOrEmpty(currentLine.ToString()))
                            && !currentLine.ToString().Trim().StartsWith("******   "))
                        {
                            if (currentLine.ToString().StartsWith('@') && lineCounter != 2)
                                comment.Append(currentLine.ToString() + Environment.NewLine);
                            else if (currentLine.ToString().Trim().StartsWith('@') && lineCounter == 2)
                                comment.Append(currentLine.ToString().Trim().Remove(0, 1));
                            else
                                comment.Append(currentLine.ToString() + " ");
                        }

                        //if (currentLine.ToString().Contains("Total Number Of Comments"))
                        //{
                        //    comment.Append(currentLine.ToString());
                        //    currentLine.Clear();
                        //    break;
                        //}
                        currentLine.Clear();
                    }
                }                             
                
            }
            
            await conn.OpenAsync();
            await sqlBulkCopyComment.WriteToServerAsync(commentsDataTable);
            await sqlBulkCopyVideo.WriteToServerAsync(videosDataTable);
            await conn.CloseAsync();

            sw.Stop();
            var elapsed = sw.Elapsed;
        }

        [HttpGet]
        [Tags("LoadDataToDatabaseFromFileReaderRegex")]
        public int LoadCommentsToDatabase()
        {
            string commentsPath = @"I:\IT\youtube\zeroCarbLifes\zeroCarbLifeComments.txt";

            var comments = System.IO.File.ReadAllText(commentsPath);

            var pattern = "^@";
            var regex = new Regex(pattern,RegexOptions.Multiline);

            var matches = regex.Matches(comments);

            return matches.Count;
        }       
    }
}
