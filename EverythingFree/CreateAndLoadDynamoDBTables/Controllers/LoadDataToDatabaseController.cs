using CoreLibrary.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.OpenApi.Validations;
using System.Data;
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
        [Tags("LoadDataToDatabaseFromStreamReaderBulkInsert")]
        public async Task GetComments(int id = 0)
        {
            string commentsPath = @"I:\IT\youtube\KenDBerry\KenDBerryComments.txt";

            var CS = "Data Source=DESKTOP-31JRUDE;Initial Catalog=YoutubeComments; Integrated Security=True;Trust Server Certificate=True";

            const string insertTitleQueryString = "INSERT INTO dbo.Videos (Id, VideoName) VALUES (@Id, @VideoName)";
            const string insertCommentQueryString = "INSERT INTO dbo.Comments (Id, Comment) VALUES (@Id, @Comment)";

            //System.IO.File.ReadAllText(commentsPath);

            DataTable commentsDataTable = new DataTable("Comments");

            DataColumn idDataColumn = new("Id",typeof(Guid));
            commentsDataTable.Columns.Add(idDataColumn);

            DataColumn commentDataColumn = new("Comment");
            commentsDataTable.Columns.Add(commentDataColumn);

            using var reader = new StreamReader(commentsPath);
            var currentLine = new StringBuilder();
            var title = new StringBuilder();
            var comment = new StringBuilder();
            var guid = new Guid();
            var comments = new List<Comment>();
            var titles = new Dictionary<Guid, string>();

            using SqlConnection conn = new SqlConnection(CS);
            using SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(conn);
            sqlBulkCopy.DestinationTableName = "dbo.Comments";
            sqlBulkCopy.ColumnMappings.Add("Id", "Id");
            sqlBulkCopy.ColumnMappings.Add("Comment", "Comment");

            var sw = new Stopwatch();
            sw.Start();

            //await conn.OpenAsync();

            while ((currentLine.Append(await reader.ReadLineAsync()) != null))
            {
                using SqlCommand commandInsertTitle = new(insertTitleQueryString, conn);
                using SqlCommand commandInsertComment = new(insertCommentQueryString, conn);

                if (string.IsNullOrEmpty(currentLine.ToString())) continue;                

                //if (currentLine.ToString().StartsWith("******   "))
                //{
                //    title.Clear();
                //    guid = Guid.NewGuid();
                //    title.Append(currentLine.ToString());
                //    titles[guid] = title.ToString();

                //    //commandInsertTitle.Parameters.AddWithValue("@Id", guid);
                //    //commandInsertTitle.Parameters.AddWithValue("@VideoName", title.ToString());

                //    //await commandInsertTitle.ExecuteNonQueryAsync();
                //}

                if (currentLine.ToString().StartsWith('@') && comment.ToString().StartsWith('@'))
                {
                    //var commentEntry = new Comment() { Guid = guid, Text = comment.ToString() };
                    //comments.Add(commentEntry);
                    commentsDataTable.Rows.Add(guid.ToString(), comment.ToString());

                    //commandInsertComment.Parameters.AddWithValue("@Id", commentEntry.Guid);
                    //commandInsertComment.Parameters.AddWithValue("@Comment", commentEntry.Text);

                    //await commandInsertComment.ExecuteNonQueryAsync();
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
            
            await conn.OpenAsync();
            await sqlBulkCopy.WriteToServerAsync(commentsDataTable);
            await conn.CloseAsync();

            sw.Stop();
            var elapsed = sw.Elapsed;
        }

        [HttpGet]
        [Tags("LoadDataToDatabaseFromFileReaderRegex")]
        public void LoadCommentsToDatabase()
        {
            string commentsPath = @"I:\IT\youtube\zeroCarbLifes\zeroCarbLifeComments.txt";

            var comments = System.IO.File.ReadAllText(commentsPath);

            var pattern = "@";
            var regex = new Regex(pattern);

            var matches = regex.Matches(comments);
        }       
    }
}
