let config = require('config');
let dbModel = require('dvp-dbmodels');
let dbHandler = require('./DBBackendHandler.js');
let logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
let async = require('async');
let mongomodels = require('dvp-mongomodels');
let RawCdr = require('dvp-mongomodels/model/Cdr').Cdr;

let companyId = config.CompanyId;
let tenantId = config.TenantId;
let startTime = config.DataMigrationStartDay;
let endTime = config.DataMigrationEndDay;

let convertCDRFromMongo = function(leg)
{
    return new Promise((resolve, reject) => {

        RawCdr.findOne({uuid: leg.Uuid}, function(err, cdrObj)
        {
            if(cdrObj._doc)
            {
                let varSec = cdrObj._doc['variables'];
                let callFlowSec = cdrObj._doc['callflow'];

                cdrObj._doc.uuid = varSec['uuid'];

                if(callFlowSec && callFlowSec.length > 0)
                {

                    let timesSec = callFlowSec[0]['times'];
                    let callerProfileSec = callFlowSec[0]['caller_profile'];

                    let uuid = varSec['uuid'];
                    let callUuid = varSec['call_uuid'];
                    let bridgeUuid = varSec['bridge_uuid'];
                    let sipFromUser = callerProfileSec['caller_id_number'];
                    let sipToUser = callerProfileSec['destination_number'];

                    if(varSec['is_ivr_transfer'])
                    {
                        sipToUser = decodeURIComponent(varSec['sip_to_user']);
                    }

                    let direction = varSec['direction'];
                    let dvpCallDirection = varSec['DVP_CALL_DIRECTION'];

                    let opCat = varSec['DVP_OPERATION_CAT'];
                    let actionCat = varSec['DVP_ACTION_CAT'];
                    let advOpAction = varSec['DVP_ADVANCED_OP_ACTION'];
                    let campaignId = varSec['CampaignId'];
                    let campaignName = varSec['CampaignName'];

                    let isAgentAnswered = false;

                    let ardsAddedTimeStamp = varSec['ards_added'];
                    let queueLeftTimeStamp = varSec['ards_queue_left'];
                    let ardsRoutedTimeStamp = varSec['ards_routed'];
                    let ardsResourceName = varSec['ards_resource_name'];
                    let ardsSipName = varSec['ARDS-SIP-Name'];
                    let sipResource = null;

                    let isQueued = false;

                    if(ardsResourceName && dvpCallDirection === 'inbound')
                    {
                        sipResource = ardsResourceName;
                    }
                    else if(ardsSipName && dvpCallDirection === 'inbound')
                    {
                        sipResource = ardsSipName;
                    }

                    if(actionCat === 'DIALER')
                    {
                        /*if(varSec['CALL_LEG_TYPE'] === 'AGENT')
                        {
                            if(varSec['sip_to_user'])
                            {
                                sipFromUser = varSec['sip_to_user'];
                                sipResource = varSec['sip_to_user'];
                            }
                            else
                            {
                                sipFromUser = varSec['dialed_user'];
                                sipResource = varSec['dialed_user'];
                            }

                            if(varSec['sip_from_user'])
                            {
                                sipToUser = varSec['sip_from_user'];
                            }
                            else
                            {
                                sipToUser = varSec['origination_caller_id_number'];
                            }
                        }*/
                        if(varSec['CALL_LEG_TYPE'] === 'CUSTOMER')
                        {
                            if(varSec['DVP_OPERATION_CAT'] === 'AGENT')
                            {
                                sipFromUser = varSec['DialerAgentSipName'];
                                sipToUser = varSec['DialerCustomerNumber'];
                            }
                            else if(varSec['DVP_OPERATION_CAT'] === 'CUSTOMER')
                            {
                                sipFromUser = varSec['dialer_from_number'];
                                sipToUser = varSec['dialer_to_number'];
                            }

                        }
                    }
                    else if(direction === 'inbound' && dvpCallDirection === 'inbound')
                    {
                        //get sip_from_user as from user for all inbound direction calls
                        sipFromUser = varSec['sip_from_user'];
                    }

                    let hangupCause = varSec['hangup_cause'];
                    let switchName = cdrObj._doc['switchname'];
                    let callerContext = callerProfileSec['context'];
                    let appId = varSec['dvp_app_id'];
                    let companyId = varSec['companyid'];
                    let tenantId = varSec['tenantid'];
                    let bUnit = varSec['business_unit'];

                    if(varSec['queue_business_unit'])
                    {
                        bUnit = varSec['queue_business_unit'];
                    }

                    let currentApp = varSec['current_application'];
                    let confName = varSec['DVP_CONFERENCE_NAME'];

                    let sipHangupDisposition = varSec['sip_hangup_disposition'];
                    let memberuuid = varSec['memberuuid'];
                    let conferenceUuid = varSec['conference_uuid'];
                    let originatedLegs = varSec['originated_legs'];
                    let startEpoch = varSec['start_epoch'];
                    let answerDate = undefined;
                    let createdDate = undefined;
                    let bridgeDate = undefined;
                    let hangupDate = undefined;

                    if(!sipToUser || (actionCat === 'FORWARDING' && direction === 'inbound'))
                    {
                        sipToUser = decodeURIComponent(varSec['sip_to_user']);
                    }

                    if(!sipFromUser)
                    {
                        sipFromUser = decodeURIComponent(varSec['origination_caller_id_number']);
                    }

                    if(!sipToUser)
                    {
                        sipToUser = decodeURIComponent(varSec['dialed_user']);
                    }

                    if(conferenceUuid)
                    {
                        callUuid = conferenceUuid;
                    }

                    sipFromUser = decodeURIComponent(sipFromUser);


                    let answeredTimeStamp = timesSec['answered_time'];
                    if(answeredTimeStamp)
                    {
                        let ansTStamp = parseInt(answeredTimeStamp)/1000;
                        answerDate = new Date(ansTStamp);
                    }

                    let createdTimeStamp = timesSec['created_time'];
                    if(createdTimeStamp)
                    {
                        let createdTStamp = parseInt(createdTimeStamp)/1000;
                        createdDate = new Date(createdTStamp);
                    }
                    else
                    {
                        if(startEpoch)
                        {
                            createdDate = new Date(startEpoch);
                        }
                    }

                    let bridgedTimeStamp = timesSec['bridged_time'];
                    if(bridgedTimeStamp)
                    {
                        let bridgedTStamp = parseInt(bridgedTimeStamp)/1000;
                        bridgeDate = new Date(bridgedTStamp);
                    }

                    let hangupTimeStamp = timesSec['hangup_time'];
                    if(hangupTimeStamp)
                    {
                        let hangupTStamp = parseInt(hangupTimeStamp)/1000;
                        hangupDate = new Date(hangupTStamp);
                    }

                    if(ardsAddedTimeStamp)
                    {
                        isQueued = true;
                    }

                    let queueTime = 0;

                    if(ardsAddedTimeStamp && queueLeftTimeStamp)
                    {
                        let ardsAddedTimeSec = parseInt(ardsAddedTimeStamp);
                        let queueLeftTimeSec = parseInt(queueLeftTimeStamp);

                        queueTime = queueLeftTimeSec - ardsAddedTimeSec;
                    }

                    if(ardsRoutedTimeStamp)
                    {
                        isAgentAnswered = true;
                    }

                    if(!appId)
                    {
                        appId = '-1';
                    }

                    if(!companyId)
                    {
                        companyId = '-1';
                    }

                    if(!tenantId)
                    {
                        tenantId = '-1';
                    }

                    if(!bUnit)
                    {
                        bUnit = 'default';
                    }

                    let ardsPriority = varSec['ards_priority'];

                    let agentSkill = '';

                    if(varSec['ards_skill_display'])
                    {
                        agentSkill = decodeURIComponent(varSec['ards_skill_display']);
                    }

                    let duration = varSec['duration'] ? parseInt(varSec['duration']) : 0;
                    let billSec = varSec['billsec'] ? parseInt(varSec['billsec']) : 0;
                    let holdSec = varSec['hold_accum_seconds'] ? parseInt(varSec['hold_accum_seconds']) : 0;
                    let progressSec = varSec['progresssec'] ? parseInt(varSec['progresssec']) : 0;
                    let answerSec = varSec['answersec'] ? parseInt(varSec['answersec']) : 0;
                    let waitSec = varSec['waitsec'] ? parseInt(varSec['waitsec']) : 0;
                    let progressMediaSec = varSec['progress_mediasec'] ? parseInt(varSec['progress_mediasec']) : 0;
                    let flowBillSec = varSec['flow_billsec'] ? parseInt(varSec['flow_billsec']) : 0;

                    let isAnswered = false;

                    if(answerDate > new Date('1970-01-01'))
                    {
                        isAnswered = true;
                    }

                    let cdr = {
                        Uuid: uuid,
                        CallUuid: callUuid,
                        MemberUuid: memberuuid,
                        BridgeUuid: bridgeUuid,
                        SipFromUser: sipFromUser,
                        SipToUser: sipToUser,
                        HangupCause: hangupCause,
                        Direction: direction,
                        SwitchName: switchName,
                        CallerContext: callerContext,
                        IsAnswered: isAnswered,
                        CreatedTime: createdDate,
                        AnsweredTime: answerDate,
                        BridgedTime: bridgeDate,
                        HangupTime: hangupDate,
                        Duration: duration,
                        BillSec: billSec,
                        HoldSec: holdSec,
                        ProgressSec: progressSec,
                        QueueSec: queueTime,
                        AnswerSec: answerSec,
                        WaitSec: waitSec,
                        ProgressMediaSec: progressMediaSec,
                        FlowBillSec: flowBillSec,
                        ObjClass: 'CDR',
                        ObjType: opCat,
                        ObjCategory: 'DEFAULT',
                        CompanyId: companyId,
                        TenantId: tenantId,
                        AppId: appId,
                        AgentSkill: agentSkill,
                        OriginatedLegs: originatedLegs,
                        DVPCallDirection: dvpCallDirection,
                        HangupDisposition:sipHangupDisposition,
                        AgentAnswered: isAgentAnswered,
                        IsQueued: isQueued,
                        SipResource: sipResource,
                        CampaignId: campaignId,
                        CampaignName: campaignName,
                        BusinessUnit: bUnit,
                        QueuePriority: ardsPriority
                    };



                    if(actionCat === 'CONFERENCE')
                    {
                        cdr.ExtraData = confName;
                    }



                    if(currentApp === 'voicemail')
                    {
                        cdr.ObjCategory = 'VOICEMAIL';
                    }
                    else if(advOpAction === 'pickup')
                    {
                        cdr.ObjCategory = 'PICKUP';
                    }

                    if(advOpAction === 'INTERCEPT')
                    {
                        cdr.ObjCategory = 'INTERCEPT';
                    }

                    if(actionCat === 'DIALER' && advOpAction)
                    {
                        cdr.ObjType = advOpAction;
                    }

                    if(varSec['DVP_ACTION_CAT'] === 'DIALER')
                    {
                        if(varSec['CALL_LEG_TYPE'] === 'CUSTOMER')
                        {
                            cdr.ObjCategory = 'DIALER'
                        }

                    }
                    else
                    {
                        if(actionCat)
                        {
                            cdr.ObjCategory = actionCat;
                        }

                    }

                    if(dvpCallDirection === 'inbound' && callFlowSec[callFlowSec.length - 1].times)
                    {
                        let callFlowTransferTime = callFlowSec[callFlowSec.length - 1].times.transfer_time;
                        let callFlowBridgeTime = callFlowSec[callFlowSec.length - 1].times.bridged_time;
                        //let callFlowAnswerTime = callFlowSec[callFlowSec.length - 1].times.answered_time;
                        let callFlowCreatedTime = callFlowSec[callFlowSec.length - 1].times.created_time;

                        if(callFlowTransferTime > 0 && callFlowCreatedTime > 0)
                        {
                            cdr.TimeAfterInitialBridge = Math.round((callFlowTransferTime - callFlowCreatedTime)/1000000);
                        }
                        else if(callFlowBridgeTime > 0 && callFlowCreatedTime > 0)
                        {
                            cdr.TimeAfterInitialBridge = Math.round((callFlowBridgeTime - callFlowCreatedTime)/1000000);
                        }
                        else
                        {
                            cdr.TimeAfterInitialBridge = 0;
                        }

                    }

                    resolve(cdr);
                }
                else
                {
                    resolve(leg);
                }

            }
            else
            {
                resolve(leg);

            }
        })

    });

};

let processBLegs = function(legInfo, cdrListArr, callback)
{
    convertCDRFromMongo(legInfo).then(cdrMongo => {

        let legType = cdrMongo.ObjType;

        if(legType && (legType === 'ATT_XFER_USER' || legType === 'ATT_XFER_GATEWAY'))
        {
            //check for Originated Legs

            if(cdrMongo.OriginatedLegs)
            {
                let decodedLegsStr = decodeURIComponent(cdrMongo.OriginatedLegs);

                let formattedStr = decodedLegsStr.replace("ARRAY::", "");

                let legsUnformattedList = formattedStr.split('|:');

                if(legsUnformattedList && legsUnformattedList.length > 0)
                {
                    let legProperties = legsUnformattedList[0].split(';');

                    let legUuid = legProperties[0];

                    dbHandler.GetSpecificLegByUuid(legUuid, function (err, transferLeg)
                    {
                        cdrListArr.push(cdrMongo);

                        if(transferLeg)
                        {
                            convertCDRFromMongo(transferLeg).then(cdrMongoTrans => {

                                let tempTransLeg = transferLeg;
                                if(cdrMongoTrans)
                                {
                                    tempTransLeg = cdrMongoTrans.toJSON();
                                }

                                tempTransLeg.IsTransferredParty = true;
                                cdrListArr.push(tempTransLeg);
                                callback(null, null);

                            });

                        }
                        else
                        {
                            callback(null, 'UUID_' + legUuid);
                        }

                    });


                }
                else
                {
                    cdrListArr.push(cdrMongo);
                    callback(null, null);
                }
            }
            else
            {
                cdrListArr.push(cdrMongo);
                callback(null, null);
            }

        }
        else
        {
            cdrListArr.push(cdrMongo);
            callback(null, null);
        }

    });

};

let collectBLegs = function(cdrListArr, uuid, callUuid, callback)
{
    let actionObj = {};
    dbHandler.GetBLegsForIVRCalls(uuid, callUuid, function(err, legInfo)
    {
        if(legInfo && legInfo.length > 0)
        {
            //DATA FOUND NEED TO COMPARE

            let asyncArr = [];

            for(i=0; i<legInfo.length; i++)
            {
                asyncArr.push(processBLegs.bind(this, legInfo[i], cdrListArr));
            }

            async.parallel(asyncArr, function(err, missingOriginatedLegs)
            {

                actionObj.SaveOnDB = true;

                callback(null, actionObj);


            });
        }
        else
        {
            actionObj.SaveOnDB = true;

            callback(null, actionObj);
        }
    })
};

let processOriginatedLegs = function(legInfo, cdrListArr, callback)
{
    convertCDRFromMongo(legInfo).then(cdrMongo => {

        let legType = cdrMongo.ObjType;

        if(legType && (legType === 'ATT_XFER_USER' || legType === 'ATT_XFER_GATEWAY'))
        {
            if(cdrMongo.OriginatedLegs)
            {
                let decodedLegsStr = decodeURIComponent(cdrMongo.OriginatedLegs);

                let formattedStr = decodedLegsStr.replace("ARRAY::", "");

                let legsUnformattedList = formattedStr.split('|:');

                if (legsUnformattedList && legsUnformattedList.length > 0)
                {
                    let legProperties = legsUnformattedList[0].split(';');

                    let legUuid = legProperties[0];

                    dbHandler.GetSpecificLegByUuid(legUuid, function (err, transferLeg)
                    {
                        cdrListArr.push(cdrMongo);

                        if(transferLeg)
                        {
                            convertCDRFromMongo(transferLeg).then(cdrMongoTrans => {

                                let tempTransLeg = transferLeg;
                                if(cdrMongoTrans)
                                {
                                    tempTransLeg = cdrMongoTrans.toJSON();
                                }
                                tempTransLeg.IsTransferredParty = true;
                                cdrListArr.push(tempTransLeg);
                                callback(null, null);

                            })


                        }
                        else
                        {
                            callback(null, 'UUID_' + legUuid);
                        }

                    })
                }
                else
                {
                    cdrListArr.push(cdrMongo);
                    callback(null, null);
                }
            }
            else
            {
                cdrListArr.push(cdrMongo);
                callback(null, null);
            }
        }
        else
        {
            cdrListArr.push(cdrMongo);
            callback(null, null);
        }

    });

};

let collectOtherLegsCDR = function(cdrListArr, relatedLegs, tryCount, mainLegId, callback)
{
    let actionObj = {};
    dbHandler.GetSpecificLegsByUuids(relatedLegs, function(allLegsFound, objList)
    {
        if(allLegsFound)
        {
            //LOOP THROUGH LEGS LIST

            let asyncArr = [];

            objList.forEach(function(legInfo)
            {
                asyncArr.push(processOriginatedLegs.bind(this, legInfo, cdrListArr));
            });

            async.parallel(asyncArr, function(err, missingLegsList){

                actionObj.SaveOnDB = true;

                callback(null, actionObj);
            });

        }
        else
        {
            actionObj.SaveOnDB = true;

            callback(null, actionObj);
        }

    });

};

let processCDRLegs = function(processedCdr, cdrList, callback)
{
    cdrList[processedCdr.Uuid] = [];
    cdrList[processedCdr.Uuid].push(processedCdr);

    let relatedLegsLength = 0;

    if(processedCdr.RelatedLegs)
    {
        relatedLegsLength = processedCdr.RelatedLegs.length;
    }

    if(processedCdr.RelatedLegs && relatedLegsLength)
    {
        console.log('=============== ORIGINATED LEGS PROCESSING =================');
        collectOtherLegsCDR(cdrList[processedCdr.Uuid], processedCdr.RelatedLegs, processedCdr.TryCount, processedCdr.Uuid, function(err, actionObj)
        {
            //Response will return false if leg need to be added back to queue
            callback(null, cdrList, actionObj);

        })
    }
    else
    {
        if(processedCdr.ObjType === 'HTTAPI' || processedCdr.ObjType === 'SOCKET' || processedCdr.ObjCategory === 'DIALER')
        {
            console.log('=============== HTTAPI LEGS PROCESSING =================');
            collectBLegs(cdrList[processedCdr.Uuid], processedCdr.Uuid, processedCdr.CallUuid, function(err, actionObj)
            {
                callback(null, cdrList, actionObj);
            })

        }
        else
        {
            console.log('================== UNKNOWN TYPE LEG CALL FOUND PROCESSING AS IT IS - UUID : ' + processedCdr.Uuid + ' ==================');

            let tempActionObj1 = {
                AddToQueue: false,
                RemoveFromRedis: false,
                SaveOnDB: true
            };
            callback(null, cdrList, tempActionObj1);
        }

    }

};

let decodeOriginatedLegs = function(cdr)
{

    try
    {
        let OriginatedLegs = cdr.OriginatedLegs;

        if(OriginatedLegs){

            //Do HTTP DECODE
            let decodedLegsStr = decodeURIComponent(OriginatedLegs);

            let formattedStr = decodedLegsStr.replace("ARRAY::", "");

            let legsUnformattedList = formattedStr.split('|:');

            cdr.RelatedLegs = [];

            for(j=0; j<legsUnformattedList.length; j++){

                let legProperties = legsUnformattedList[j].split(';');

                let legUuid = legProperties[0];

                if(cdr.Uuid != legUuid && !(cdr.RelatedLegs.indexOf(legUuid) > -1)){

                    cdr.RelatedLegs.push(legUuid);
                }

            }
        }

        return cdr;
    }
    catch(ex)
    {
        return null;
    }
};



let processCampaignCDR = function(primaryLeg, curCdr)
{
    let cdrAppendObj = {};
    let callHangupDirectionA = '';
    let callHangupDirectionB = '';
    let isOutboundTransferCall = false;
    let holdSecTemp = 0;

    let callCategory = '';


    //Need to filter out inbound and outbound legs before processing

    let firstLeg = primaryLeg;

    if(firstLeg)
    {
        //Process First Leg
        callHangupDirectionA = firstLeg.HangupDisposition;

        if(firstLeg.ObjType === 'ATT_XFER_USER' || firstLeg.ObjType === 'ATT_XFER_GATEWAY')
        {
            isOutboundTransferCall = true;
        }

        cdrAppendObj.Uuid = firstLeg.Uuid;
        cdrAppendObj.RecordingUuid = firstLeg.Uuid;
        cdrAppendObj.CallUuid = firstLeg.CallUuid;
        cdrAppendObj.BridgeUuid = firstLeg.BridgeUuid;
        cdrAppendObj.SwitchName = firstLeg.SwitchName;
        cdrAppendObj.SipFromUser = firstLeg.SipFromUser;
        cdrAppendObj.SipToUser = firstLeg.SipToUser;
        cdrAppendObj.RecievedBy = firstLeg.SipToUser;
        cdrAppendObj.CallerContext = firstLeg.CallerContext;
        cdrAppendObj.HangupCause = firstLeg.HangupCause;
        cdrAppendObj.CreatedTime = firstLeg.CreatedTime;
        cdrAppendObj.Duration = firstLeg.Duration;
        cdrAppendObj.BridgedTime = firstLeg.BridgedTime;
        cdrAppendObj.HangupTime = firstLeg.HangupTime;
        cdrAppendObj.AppId = firstLeg.AppId;
        cdrAppendObj.CompanyId = firstLeg.CompanyId;
        cdrAppendObj.TenantId = firstLeg.TenantId;
        cdrAppendObj.ExtraData = firstLeg.ExtraData;
        cdrAppendObj.IsQueued = firstLeg.IsQueued;
        cdrAppendObj.IsAnswered = false;
        cdrAppendObj.CampaignName = firstLeg.CampaignName;
        cdrAppendObj.CampaignId = firstLeg.CampaignId;
        cdrAppendObj.BillSec = 0;
        cdrAppendObj.HoldSec = 0;
        cdrAppendObj.ProgressSec = 0;
        cdrAppendObj.FlowBillSec = 0;
        cdrAppendObj.ProgressMediaSec = 0;
        cdrAppendObj.WaitSec = 0;
        cdrAppendObj.AnswerSec = 0;
        cdrAppendObj.IsAnswered = firstLeg.IsAnswered;
        cdrAppendObj.BillSec = firstLeg.BillSec;

        holdSecTemp = holdSecTemp + firstLeg.HoldSec;

        if(firstLeg.ObjType === 'BLAST' || firstLeg.ObjType === 'DIRECT' || firstLeg.ObjType === 'IVRCALLBACK')
        {
            callHangupDirectionB = firstLeg.HangupDisposition;
        }

        cdrAppendObj.DVPCallDirection = 'outbound';


        if(firstLeg.ProgressSec)
        {
            cdrAppendObj.ProgressSec = firstLeg.ProgressSec;
        }

        if(firstLeg.FlowBillSec)
        {
            cdrAppendObj.FlowBillSec = firstLeg.FlowBillSec;
        }

        if(firstLeg.ProgressMediaSec)
        {
            cdrAppendObj.ProgressMediaSec = firstLeg.ProgressMediaSec;
        }

        if(firstLeg.WaitSec)
        {
            cdrAppendObj.WaitSec = firstLeg.WaitSec;
        }

        cdrAppendObj.QueueSec = firstLeg.QueueSec;
        cdrAppendObj.AgentSkill = firstLeg.AgentSkill;

        cdrAppendObj.AnswerSec = firstLeg.AnswerSec;
        cdrAppendObj.AnsweredTime = firstLeg.AnsweredTime;

        cdrAppendObj.ObjType = firstLeg.ObjType;
        cdrAppendObj.ObjCategory = firstLeg.ObjCategory;
    }

    //process other legs

    let otherLegs = curCdr.filter(function (item) {
        return item.ObjCategory !== 'DIALER';

    });

    if(otherLegs && otherLegs.length > 0)
    {
        let customerLeg = otherLegs.find(function (item) {
            return item.ObjType === 'CUSTOMER';
        });

        let agentLeg = otherLegs.find(function (item) {
            return (item.ObjType === 'AGENT' || item.ObjType === 'PRIVATE_USER');
        });

        if(customerLeg)
        {
            cdrAppendObj.BillSec = customerLeg.BillSec;
            cdrAppendObj.AnswerSec = customerLeg.AnswerSec;

            holdSecTemp = holdSecTemp + customerLeg.HoldSec;

            callHangupDirectionB = customerLeg.HangupDisposition;

            cdrAppendObj.IsAnswered = customerLeg.IsAnswered;

            cdrAppendObj.IsQueued = customerLeg.IsQueued;

        }

        if(agentLeg)
        {
            holdSecTemp = holdSecTemp + agentLeg.HoldSec;
            callHangupDirectionB = agentLeg.HangupDisposition;
            cdrAppendObj.AnswerSec = agentLeg.AnswerSec;

            if(firstLeg.ObjType !== 'AGENT')
            {
                cdrAppendObj.AgentAnswered = agentLeg.IsAnswered;
                cdrAppendObj.IsAnswered = agentLeg.IsAnswered;
            }
        }

        cdrAppendObj.HoldSec = holdSecTemp;
        cdrAppendObj.IvrConnectSec = 0;

    }

    if(!cdrAppendObj.IsAnswered)
    {
        cdrAppendObj.AnswerSec = cdrAppendObj.Duration;
    }


    if (callHangupDirectionA === 'recv_bye' || callHangupDirectionA === 'recv_refuse') {
        cdrAppendObj.HangupParty = 'CALLEE';
    }
    else if (callHangupDirectionB === 'recv_bye' || callHangupDirectionB === 'recv_refuse') {
        cdrAppendObj.HangupParty = 'CALLER';
    }
    else {
        cdrAppendObj.HangupParty = 'SYSTEM';
    }


    return cdrAppendObj;

};

let processSingleCdrLeg = function(primaryLeg, callback)
{
    convertCDRFromMongo(primaryLeg).then(cdrMongo => {

        let cdr = decodeOriginatedLegs(cdrMongo);

        let cdrList = {};

        //processCDRLegs method should immediately stop execution and return missing leg uuids or return the related cdr legs
        processCDRLegs(cdr, cdrList, function(err, resp, actionObj)
        {
            if(err)
            {
                logger.error('[DVP-CDRProcessor.processSingleCdrLeg] - [%s] - Error occurred while processing CDR Legs', err);
            }
            if(actionObj.SaveOnDB)
            {
                let cdrAppendObj = {};
                let primaryLeg = cdr;
                let isOutboundTransferCall = false;

                if(primaryLeg.DVPCallDirection === 'outbound' && (primaryLeg.ObjType === 'ATT_XFER_USER' || primaryLeg.ObjType === 'ATT_XFER_GATEWAY'))
                {
                    isOutboundTransferCall = true;
                }

                let curCdr = resp[Object.keys(resp)[0]];

                if(cdr.ObjCategory === 'DIALER')
                {
                    cdrAppendObj =  processCampaignCDR(primaryLeg, curCdr);
                }
                else
                {
                    let outLegAnswered = false;

                    let callHangupDirectionA = '';
                    let callHangupDirectionB = '';

                    //Need to filter out inbound and outbound legs before processing

                    /*let filteredInb = curCdr.filter(function (item)
                     {
                     if (item.Direction === 'inbound')
                     {
                     return true;
                     }
                     else
                     {
                     return false;
                     }

                     });*/

                    let secondaryLeg = null;

                    let filteredOutb = curCdr.filter(function (item)
                    {
                        return item.Direction === 'outbound';
                    });


                    let transferredParties = '';

                    let transferCallOriginalCallLeg = null;

                    let transLegInfo = {
                        transferLegB: [],
                        actualTransferLegs: []
                    };

                    filteredOutb.reduce(function(accumilator, currentValue)
                    {
                        if(isOutboundTransferCall)
                        {
                            if (currentValue.ObjType !== 'ATT_XFER_USER' && currentValue.ObjType !== 'ATT_XFER_GATEWAY')
                            {
                                accumilator.transferLegB.push(currentValue);
                            }
                            else
                            {
                                accumilator.actualTransferLegs.push(currentValue);
                            }

                        }
                        else
                        {
                            if ((currentValue.ObjType === 'ATT_XFER_USER' || currentValue.ObjType === 'ATT_XFER_GATEWAY') && !currentValue.IsTransferredParty)
                            {
                                accumilator.transferLegB.push(currentValue);
                            }

                            if(currentValue.IsTransferredParty)
                            {
                                accumilator.actualTransferLegs.push(currentValue);
                            }
                        }
                        return accumilator;

                    }, transLegInfo);

                    if(transLegInfo && transLegInfo.actualTransferLegs && transLegInfo.actualTransferLegs.length > 0 && transLegInfo.transferLegB && transLegInfo.transferLegB.length > 0)
                    {
                        transLegInfo.actualTransferLegs.forEach(function(actualTransLeg)
                        {
                            let index = transLegInfo.transferLegB.map(function(e) { return e.Uuid }).indexOf(actualTransLeg.Uuid);

                            if(index > -1)
                            {
                                transLegInfo.transferLegB.splice(index, 1);
                            }

                        })
                    }


                    if(transLegInfo.transferLegB && transLegInfo.transferLegB.length > 0)
                    {

                        let transferLegBAnswered = transLegInfo.transferLegB.filter(function (item) {
                            return item.IsAnswered === true;
                        });

                        if(transferLegBAnswered && transferLegBAnswered.length > 0)
                        {
                            transferCallOriginalCallLeg = transferLegBAnswered[0];
                        }
                        else
                        {
                            transferCallOriginalCallLeg = transLegInfo.transferLegB[0];
                        }
                    }

                    let callCategory = primaryLeg.ObjCategory;

                    if(transferCallOriginalCallLeg)
                    {
                        secondaryLeg = transferCallOriginalCallLeg;

                        for(k = 0; k < transLegInfo.actualTransferLegs.length; k++)
                        {
                            transferredParties = transferredParties + transLegInfo.actualTransferLegs[k].SipToUser + ',';
                        }
                    }
                    else
                    {
                        if(filteredOutb.length > 1)
                        {
                            let filteredOutbAnswered = filteredOutb.filter(function (item2)
                            {
                                return item2.IsAnswered;
                            });

                            if(filteredOutbAnswered && filteredOutbAnswered.length > 0)
                            {
                                if(filteredOutbAnswered.length > 1)
                                {
                                    //check for bridged calles
                                    let filteredOutbBridged = filteredOutbAnswered.filter(function (item3)
                                    {
                                        return item3.BridgedTime > new Date('1970-01-01');
                                    });

                                    if(filteredOutbBridged && filteredOutbBridged.length > 0)
                                    {
                                        secondaryLeg = filteredOutbBridged[0];
                                    }
                                    else
                                    {
                                        secondaryLeg = filteredOutbAnswered[0];
                                    }

                                }
                                else
                                {
                                    secondaryLeg = filteredOutbAnswered[0];
                                }

                            }
                            else
                            {
                                secondaryLeg = filteredOutb[0];
                            }
                        }
                        else
                        {
                            if(filteredOutb && filteredOutb.length > 0)
                            {
                                secondaryLeg = filteredOutb[0];

                                if(callCategory === 'FOLLOW_ME' || callCategory === 'FORWARDING')
                                {
                                    for (k = 0; k < filteredOutb.length; k++) {
                                        transferredParties = transferredParties + filteredOutb[k].SipToUser + ',';
                                    }

                                }


                            }
                        }
                    }

                    if(cdrAppendObj.ObjType === 'FAX_INBOUND')
                    {
                        cdrAppendObj.IsAnswered = primaryLeg.IsAnswered;
                    }

                    //process primary leg first

                    //process common data

                    cdrAppendObj.Uuid = primaryLeg.Uuid;
                    cdrAppendObj.RecordingUuid = primaryLeg.Uuid;
                    cdrAppendObj.CallUuid = primaryLeg.CallUuid;
                    cdrAppendObj.BridgeUuid = primaryLeg.BridgeUuid;
                    cdrAppendObj.SwitchName = primaryLeg.SwitchName;
                    cdrAppendObj.SipFromUser = primaryLeg.SipFromUser;
                    cdrAppendObj.SipToUser = primaryLeg.SipToUser;
                    cdrAppendObj.CallerContext = primaryLeg.CallerContext;
                    cdrAppendObj.HangupCause = primaryLeg.HangupCause;
                    cdrAppendObj.CreatedTime = primaryLeg.CreatedTime;
                    cdrAppendObj.Duration = primaryLeg.Duration;
                    cdrAppendObj.BridgedTime = primaryLeg.BridgedTime;
                    cdrAppendObj.HangupTime = primaryLeg.HangupTime;
                    cdrAppendObj.AppId = primaryLeg.AppId;
                    cdrAppendObj.CompanyId = primaryLeg.CompanyId;
                    cdrAppendObj.TenantId = primaryLeg.TenantId;
                    cdrAppendObj.ExtraData = primaryLeg.ExtraData;
                    cdrAppendObj.IsQueued = primaryLeg.IsQueued;
                    cdrAppendObj.BusinessUnit = primaryLeg.BusinessUnit;
                    cdrAppendObj.QueuePriority = primaryLeg.QueuePriority;

                    cdrAppendObj.AgentAnswered = primaryLeg.AgentAnswered;

                    if (primaryLeg.DVPCallDirection)
                    {
                        callHangupDirectionA = primaryLeg.HangupDisposition;
                    }

                    cdrAppendObj.IsAnswered = false;


                    cdrAppendObj.BillSec = 0;
                    cdrAppendObj.HoldSec = 0;
                    cdrAppendObj.ProgressSec = 0;
                    cdrAppendObj.FlowBillSec = 0;
                    cdrAppendObj.ProgressMediaSec = 0;
                    cdrAppendObj.WaitSec = 0;

                    if(primaryLeg.ProgressSec)
                    {
                        cdrAppendObj.ProgressSec = primaryLeg.ProgressSec;
                    }

                    if(primaryLeg.FlowBillSec)
                    {
                        cdrAppendObj.FlowBillSec = primaryLeg.FlowBillSec;
                    }

                    if(primaryLeg.ProgressMediaSec)
                    {
                        cdrAppendObj.ProgressMediaSec = primaryLeg.ProgressMediaSec;
                    }

                    if(primaryLeg.WaitSec)
                    {
                        cdrAppendObj.WaitSec = primaryLeg.WaitSec;
                    }


                    cdrAppendObj.DVPCallDirection = primaryLeg.DVPCallDirection;

                    cdrAppendObj.HoldSec = cdrAppendObj.HoldSec +  primaryLeg.HoldSec;

                    /*if (primaryLeg.DVPCallDirection === 'inbound')
                     {
                     cdrAppendObj.HoldSec = primaryLeg.HoldSec;
                     }*/


                    cdrAppendObj.QueueSec = primaryLeg.QueueSec;
                    cdrAppendObj.AgentSkill = primaryLeg.AgentSkill;

                    cdrAppendObj.AnswerSec = primaryLeg.AnswerSec;
                    cdrAppendObj.AnsweredTime = primaryLeg.AnsweredTime;

                    cdrAppendObj.ObjType = primaryLeg.ObjType;
                    cdrAppendObj.ObjCategory = primaryLeg.ObjCategory;
                    cdrAppendObj.TimeAfterInitialBridge = primaryLeg.TimeAfterInitialBridge;

                    //process outbound legs next

                    if(secondaryLeg)
                    {
                        if (secondaryLeg.BillSec > 0)
                        {
                            outLegAnswered = true;
                        }

                        if(cdrAppendObj.DVPCallDirection === 'inbound' && outLegAnswered)
                        {
                            cdrAppendObj.BillSec = primaryLeg.Duration - primaryLeg.TimeAfterInitialBridge;
                        }

                        if(cdrAppendObj.DVPCallDirection === 'outbound')
                        {
                            cdrAppendObj.RecordingUuid = secondaryLeg.Uuid;
                        }

                        callHangupDirectionB = secondaryLeg.HangupDisposition;

                        cdrAppendObj.RecievedBy = secondaryLeg.SipToUser;

                        cdrAppendObj.AnsweredTime = secondaryLeg.AnsweredTime;


                        cdrAppendObj.HoldSec = cdrAppendObj.HoldSec + secondaryLeg.HoldSec;
                        /*if (primaryLeg.DVPCallDirection === 'outbound')
                         {
                         cdrAppendObj.HoldSec = secondaryLeg.HoldSec;
                         }*/

                        if(cdrAppendObj.DVPCallDirection === 'outbound')
                        {
                            cdrAppendObj.BillSec = secondaryLeg.BillSec;
                        }

                        if (!cdrAppendObj.ObjType)
                        {
                            cdrAppendObj.ObjType = secondaryLeg.ObjType;
                        }

                        if (!cdrAppendObj.ObjCategory)
                        {
                            cdrAppendObj.ObjCategory = secondaryLeg.ObjCategory;
                        }

                        cdrAppendObj.AnswerSec = secondaryLeg.AnswerSec;

                        if(!outLegAnswered && cdrAppendObj.RecievedBy)
                        {
                            cdrAppendObj.AnswerSec = secondaryLeg.Duration;
                        }

                        if(transferredParties)
                        {
                            transferredParties = transferredParties.slice(0, -1);
                            cdrAppendObj.TransferredParties = transferredParties;
                        }
                    }

                    /*if(transferCallOriginalCallLeg)
                     {
                     cdrAppendObj.SipFromUser = transferCallOriginalCallLeg.SipFromUser;
                     }*/


                    cdrAppendObj.IvrConnectSec = cdrAppendObj.Duration - cdrAppendObj.QueueSec - cdrAppendObj.BillSec;


                    cdrAppendObj.IsAnswered = outLegAnswered;


                    if (callHangupDirectionA === 'recv_bye' || callHangupDirectionA === 'recv_cancel')
                    {
                        cdrAppendObj.HangupParty = 'CALLER';
                    }
                    else if (callHangupDirectionB === 'recv_bye' || callHangupDirectionB === 'recv_refuse')
                    {
                        cdrAppendObj.HangupParty = 'CALLEE';
                    }
                    else
                    {
                        cdrAppendObj.HangupParty = 'SYSTEM';
                    }
                }

                dbHandler.AddProcessedCDR(cdrAppendObj, function(err, addResp)
                {
                    callback(err, actionObj);

                });
            }
            else
            {
                callback(null, actionObj);
            }


        })

    });

};

let offset = 0;

let arr = ['HTTAPI', 'SOCKET', 'REJECTED', 'FAX_INBOUND'];

let getCDRPrimaryLegs = function(){

    //Condition to Skip
    let executionArr = [];


    dbModel.CallCDR.findAll({where :[{Direction: 'inbound', CompanyId: companyId, TenantId: tenantId, CreatedTime:{between:[startTime, endTime]}}], order:[['CreatedTime','ASC']], limit: 5, offset: offset}).then(function(callLegs)
    {
        if(callLegs && callLegs.length > 0)
        {
            callLegs.forEach(callLeg => {
                if(callLeg.Direction === 'inbound' && callLeg.ObjCategory !== 'CONFERENCE' && (callLeg.OriginatedLegs !== null ||
                        (callLeg.OriginatedLegs === null && (arr.indexOf(callLeg.ObjType) > -1 || callLeg.ObjCategory === 'DND' || callLeg.ObjCategory === 'OUTBOUND_DENIED'))))
                {
                    executionArr.push(processSingleCdrLeg.bind(this, callLeg));
                }
                offset++;

            });

            async.series(executionArr, function(err, callback){

                console.log('CDRS PROCESSED : ' + offset);

                setTimeout(getCDRPrimaryLegs, 5000);

            });



        }
        else
        {
            console.log("TERMINATING OPERATION");
        }

    });



};

//getCDRPrimaryLegs();



///////////////////////////////////////// UPDATE ACW RESOURCE INFO /////////////////////////////

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

                setTimeout(getACWDetails, 5000);

            });



        }
        else
        {
            console.log("TERMINATING OPERATION");
        }

    });



};

getACWDetails();

//getCDRPrimaryLegs();

