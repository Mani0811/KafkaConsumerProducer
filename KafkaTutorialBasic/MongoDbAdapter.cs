using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaTutorialBasic
{

    public class DataAccess
    {
        MongoClient _client;
        MongoServer _server;
        MongoDatabase _db;

        public DataAccess()
        {
            _client = new MongoClient("mongodb://localhost:27017");
#pragma warning disable CS0618 // Type or member is obsolete
            _server = _client.GetServer();
#pragma warning restore CS0618 // Type or member is obsolete
            _db = _server.GetDatabase("EmployeeDB");
        }

        public Provider Create(Provider p)
        {
            _db.GetCollection<Provider>("Provider").Save(p);
            return p;
        }

        public void Update(Provider provider)
        {
            var res = Query<Provider>.EQ(p => p.Id, provider.Id);
            var operation = Update<Provider>.Replace(provider);
            _db.GetCollection<Provider>("Provider").Update(res,operation);
        }
        public IEnumerable<Provider> GetAll()
        {
            var providerList = _db.GetCollection<Provider>("Provider").FindAll();
            return providerList;
        }

        public Provider GetProviderById(string id)
        {
            ObjectId objId = new ObjectId(id);
            var res = Query<Provider>.EQ(p => p.Id, objId);
            return _db.GetCollection<Provider>("Provider").FindOne(res);
        }

        public void IncrementProviderImageResizeQueueCount(ObjectId id, Provider p)
        {
            p.Id = id;
            var res = Query<Provider>.EQ(pd => pd.Id, id);
            var operation = Update<Provider>.Inc(p => p.ImageQueueCount, 1);
            _db.GetCollection<Provider>("Provider").Update(res, operation);
        }

        public void DecrementProviderQueueCount(ObjectId id, Provider p)
        {
            p.Id = id;
            var res = Query<Provider>.EQ(pd => pd.Id, id);
            var operation = Update<Provider>.Inc(p => p.ImageQueueCount, -1);
            _db.GetCollection<Provider>("Provider").Update(res, operation);
        }
    }
    public class Provider
    {
        public ObjectId Id { get; set; }

        [BsonElement("ImageQueueCount")]
        public int ImageQueueCount { get; set; }

    }
}
