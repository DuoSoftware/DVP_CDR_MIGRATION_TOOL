///////////////////////////////////////// UPDATE ACW RESOURCE INFO /////////////////////////////

let config = require('config');
let dbModel = require('dvp-dbmodels');
let async = require('async');
let mongomodels = require('dvp-mongomodels');
let RawCdr = require('dvp-mongomodels/model/Cdr').Cdr;

let companyId = config.CompanyId;
let tenantId = config.TenantId;
let startTime = config.DataMigrationStartDay;
let endTime = config.DataMigrationEndDay;
let rotateSpeed = config.RotateSpeed;

let offset = 0;

let findPrimaryLegCDRFromMongo = function(uuid)
{
    return new Promise((resolve, reject) => {

        RawCdr.findOne({uuid: uuid}, function(err, cdrObj)
        {
            if(cdrObj._doc) {
                let varSec = cdrObj._doc['variables'];
                let callFlowSec = cdrObj._doc['callflow'];

                if (varSec['direction'] === 'outbound')
                {
                    //find relevant inbound leg
                    let orgUuid = varSec['originating_leg_uuid'];

                    resolve(orgUuid);
                }
                else{
                    resolve(null);

                }

            }
            else
            {
                resolve(null);

            }
        })

    });

};


let processSingleAcwInfo = function(acwInfo, callback)
{
    findPrimaryLegCDRFromMongo(acwInfo.SessionId).then(uuidToReplace => {

        if(uuidToReplace)
        {
            console.log('REWRITING SESSION ID : ' + uuidToReplace);
            acwInfo.updateAttributes({SessionId: uuidToReplace}).then(function (resUpdate)
            {
                callback(null, true);

            }).catch(function (err)
            {

                callback(null, true);

            });
        }
        else{
            callback(null, true);
        }



    });

};

let getACWDetails = function(){

    //Condition to Skip
    let executionArr = [];


    dbModel.ResResourceAcwInfo.findAll({where :[{CompanyId: companyId, TenantId: tenantId, createdAt:{between:[startTime, endTime]}}], order:[['createdAt','ASC']], limit: 5, offset: offset}).then(function(acwInfos)
    {
        if(acwInfos && acwInfos.length > 0)
        {
            acwInfos.forEach(acwInfo => {

                executionArr.push(processSingleAcwInfo.bind(this, acwInfo));
                offset++;

            });

            async.series(executionArr, function(err, callback){

                console.log('ACWS PROCESSED : ' + offset);

                setTimeout(getACWDetails, rotateSpeed);

            });



        }
        else
        {
            console.log("TERMINATING OPERATION");
        }

    });



};

getACWDetails();