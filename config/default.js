module.exports = {

    "DB": {
        "Type":"postgres",
        "User":"",
        "Password":"",
        "Port":5000,
        "Host":"",
        "Database":""
    },

    "Redis":
        {
            "mode":"",//instance, cluster, sentinel
            "ip": "",
            "port": 6379,
            "user": "",
            "password": "",
            "sentinels":{
                "hosts": "",
                "port":26379,
                "name":""
            }

        },

    "Security":
        {

            "ip" : "",
            "port": 6379,
            "user": "",
            "password": "",
            "mode":"",//instance, cluster, sentinel
            "sentinels":{
                "hosts": "",
                "port":26379,
                "name":""
            }
        },

    "Mongo":
        {
            "ip":"",
            "port":"",
            "dbname":"",
            "password":"",
            "user":"",
            "replicaset" :""
        },

    "RabbitMQ":
        {
            "ip": "",
            "port": 5672,
            "user": "",
            "password": "",
            "vhost":'/'
        },

    "Services":
        {

            "fileServiceHost": "",
            "fileServicePort": 8812,
            "fileServiceVersion":""

        },

    "Host":{
        "Ip":"",
        "Port":9093,
        "Version":""
    },

  "Token":"",
  "SendAbandonCallsToQueue":true,
  "DataMigrationStartDay": "",
  "DataMigrationEndDay": "",
  "CompanyId": 14,
  "TenantId": 1,
  "RotateSpeed": 3000

};
