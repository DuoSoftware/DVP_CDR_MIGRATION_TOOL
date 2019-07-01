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


let processSingleResInfo = function(resInfo, callback)
{
    findPrimaryLegCDRFromMongo(resInfo.SessionId).then(uuidToReplace => {

        if(uuidToReplace)
        {
            console.log('REWRITING SESSION ID : ' + uuidToReplace);
            resInfo.updateAttributes({SessionId: uuidToReplace}).then(function (resUpdate)
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

let getResTaskRejectInfoDetails = function(){

    //Condition to Skip
    let executionArr = [];


    dbModel.ResResourceTaskRejectInfo.findAll({where :[{CompanyId: companyId, TenantId: tenantId, createdAt:{between:[startTime, endTime]}}], order:[['createdAt','ASC']], limit: 5, offset: offset}).then(function(resInfos)
    {
        if(resInfos && resInfos.length > 0)
        {
            resInfos.forEach(resInfo => {

                executionArr.push(processSingleResInfo.bind(this, resInfo));
                offset++;

            });

            async.series(executionArr, function(err, callback){

                console.log('RES TASK REJECT INFOS PROCESSED : ' + offset);

                setTimeout(getResTaskRejectInfoDetails, rotateSpeed);

            });



        }
        else
        {
            console.log("TERMINATING OPERATION");
        }

    });



};

getResTaskRejectInfoDetails();