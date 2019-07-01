module.exports = {

    "DB": {
        "Type":"postgres",
        "User":"duo",
        "Password":"DuoS123",
        "Port":5432,
        "Host":"104.236.231.11",
        "Database":"duo"
    },

    "Redis":
        {
            "mode":"instance",//instance, cluster, sentinel
            "ip": "138.197.90.92",
            "port": 6389,
            "user": "duo",
            "password": "DuoS123",
            "sentinels":{
                "hosts": "138.197.90.92,45.55.205.92,138.197.90.92",
                "port":16389,
                "name":"redis-cluster"
            }

        },

    "Security":
        {

            "ip" : "45.55.142.207",
            "port": 6389,
            "user": "duo",
            "password": "DuoS123",
            "mode":"sentinel",//instance, cluster, sentinel
            "sentinels":{
                "hosts": "138.197.90.92,45.55.205.92,138.197.90.92",
                "port":16389,
                "name":"redis-cluster"
            }
        },

    "Mongo":
        {
            "ip":"104.236.231.11",
            "port":"27017",
            "dbname":"dvpdb",
            "password":"DuoS123",
            "user":"duo",
            "replicaset" :"104.236.231.11"
        },

    "RabbitMQ":
        {
            "ip": "45.55.142.207",
            "port": 5672,
            "user": "admin",
            "password": "admin",
            "vhost":'/'
        },

    "Services":
        {

            "fileServiceHost": "fileservice.app.veery.cloud",
            "fileServicePort": 5649,
            "fileServiceVersion":"1.0.0.0"

        },

    "Host":{
        "Ip":"0.0.0.0",
        "Port":9093,
        "Version":"1.0.0.0"
    },

  "Token":"",
  "SendAbandonCallsToQueue":true,
  "DataMigrationStartDay": "2019-06-12 00:00:00+05:30",
  "DataMigrationEndDay": "2019-06-20 23:59:59+05:30",
  "CompanyId": 103,
  "TenantId": 1,
  "RotateSpeed": 2000
};
