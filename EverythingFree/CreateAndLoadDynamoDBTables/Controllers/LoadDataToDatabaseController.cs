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
            string pathOfDirectory = @"I:\IT\youtube\!!!ALL\commentsWithDates";
            var files = Directory.GetFiles(pathOfDirectory, "*.*", SearchOption.TopDirectoryOnly);

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

            var videoId = new Guid();                   
            var comment = new StringBuilder();  
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

                        if (line == null) break;
                        
                        if (line.StartsWith("******   "))
                        {
                            videoId = Guid.NewGuid();
                            videosDataTable.Rows.Add(videoId, line.ToString());
                            continue;
                        }

                        comment.Append(line);

                        if (line.EndsWith("---eoc"))
                        {
                            commentsDataTable.Rows.Add(videoId, comment.ToString());
                            comment.Clear();
                        }
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
