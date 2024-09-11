using DataLayer.Interfaces;
using Microsoft.AspNetCore.Mvc;

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
        public string Get(int id)
        {
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
