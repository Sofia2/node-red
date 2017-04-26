module.exports = function(RED) {
    "use strict";
    var util = require("util");
    var vm = require("vm");
    var request = require('sync-request');
	var sofia2Config = require('./config/sofia2-config');

    var urlService = sofia2Config.sibBasePath + '/sib/services/api_ssap/v01/SSAPResource';

    var cipherKey = null;
    var kpName = "";
    var token = "";
    var thinKp ="";
    var instanciakp = "";
    var ontology = "";
    var sessionKey = "";
    var data = "";
    var insertResponse = "";
    var joinResponse = "";
    //Buffer de peticiones pendientes para envio por websocket
    var pendingWSRequestsBuffer = [];




    /**
     * Procesa los datos del nodo para enviar el mensaje
     * @author clobato
     */
    function sendResults(node, _msgid, msgs) {
        console.log("");




        if (msgs != null && msgs != undefined) {


            data = msgs.payload;
            ontology = msgs.ontology;
            
        }

        if (node != null && node != undefined) {
            if (node.name != undefined) {
                kpName = node.name;
                //console.log("Name KP: "+ kpName)
            }
            if (node.token != undefined) {
                token = node.token;
                //console.log("Token: " + token);
            }
            if (node.instanciakp != undefined) {
                instanciakp = node.instanciakp;
                //console.log("Instance Kp: " + instanciakp);
            }
            if(node.thinKp !=undefined){
                thinKp = node.thinKp;
            }


            //Insert
            insert(data, ontology, insertResponse);


        }




    }


    /**
     * Construye query INSERT a enviar al SIB 
     * @author clobato
     */
    function insert(data, ontology, insertResponse) {

        //console.log("****INSERT****");
        //data = addQuotesToData(data);


        data = data.replace(/\\/g, "\\\\")
            .replace(/'/g, "\\'")
            .replace(/"/g, "\\\"");




        data = '"' + data + '"';

        var queryInsert = '{"body":{"data":' +
            data +
            ',"query":null},"direction":"REQUEST","messageType":"INSERT","ontology":"' +
            ontology + '","sessionKey":"' + sessionKey + '"}';

        /*console.log("");
        console.log("Query to Insert:" + queryInsert);
        console.log("");*/


        sendMessage("INSERT", queryInsert, false, insertResponse);


    }


    /**
     * Construye query JOIN a enviar al SIB websocket
     * @author clobato
     */
    function join(data, ontology, joinResponse) {

        //console.log("****JOIN****");

        var queryJoin = '{"body":{"instance":"' +
            thinKp+":"+instanciakp +
            '","token":"' +
            token +
            '"},"direction":"REQUEST","messageType":"JOIN","sessionKey":null}';



        /*console.log("");
        console.log("Query to Join:" + queryJoin);
        console.log("");*/
        sendMessage("JOIN", queryJoin, false, joinResponse);


    }



   /**
     * Construye msg a enviar al SIB  por REST
     * @author clobato
     */
    function insertarMsg(sessionKey, ontology, data) {

        console.log("Send msg Nodo [sofia2-kp-insert]");

       
        
        var insertResponse = request('POST', urlService, {
            json: {
                'sessionKey': sessionKey,
                'ontology': ontology,
                'data': data

            }
        });
        
        
        if (insertResponse != null || insertResponse != undefined) {

            var responseINSERT = JSON.parse(insertResponse.getBody('utf8'));
            
            //si no se ha tenido respuesta se vuelve a llamar.
            if(responseINSERT ==null || responseINSERT == undefined){
                insertarMsg(sessionKey,ontology,data);
            }
        }


    }



    /**
     * Envia el mensaje construido al SIB de Sofia 2
     * @author clobato
     */


    function sendMessage(_tipoQuery, _query, _cipherMessage, _responseCallback) {

        var messageSSAP = JSON.parse(_query);

        console.log("Message SSAPP", messageSSAP);

        if (messageSSAP.messageType == "INSERT" && (messageSSAP.sessionKey == undefined || messageSSAP.sessionKey == '')) {

            
            var joinResponse = request('POST', urlService, {
                json: {
                    'join': true,
                    'instanceKP': thinKp+":"+instanciakp,
                    'token': token
                }
            });


            if (joinResponse != null || joinResponse != undefined) {
                var responseJOIN = JSON.parse(joinResponse.getBody('utf8'));

                if (responseJOIN.sessionKey != null) {

                    sessionKey = responseJOIN.sessionKey;

                    //realizo el insert
                    insertarMsg(sessionKey, ontology, data);


                }

            }


        } else if (messageSSAP.messageType == "INSERT" && (messageSSAP.sessionKey != null || messageSSAP.sessionKey != undefined)) {


            insertarMsg(sessionKey, ontology, data);
        

        }


    }




    /**
     * 
     * @author clobato
     */
    function addQuotesToData(data) {
        if (data.indexOf("{") != 0)
            data = "{" + data + "}";

        return data;
    }


    /**
     * Crea el nodo SOFIA2-SIB
     * @author clobato
     */
    function Sofia2SIB(n) {
        RED.nodes.createNode(this, n);
        var node = this;

        this.name = n.name;
        this.token = n.token;
        this.instanciakp = n.instanciakp;
        this.thinKp= n.thinKp;
        this.func = n.func;
        var functionText = "var results = null;" +
            "results = (function(msg){ " +
            "var __msgid__ = msg._msgid;" +
            "var node = {" +
            "log:__node__.log," +
            "error:__node__.error," +
            "warn:__node__.warn," +
            "on:__node__.on," +
            "status:__node__.status," +
            "send:function(msgs){ __node__.send(__msgid__,msgs);}" +
            "};\n" +
            this.func + "\n" +
            "})(msg);";

        this.outstandingTimers = [];
        this.outstandingIntervals = [];
        var sandbox = {
            console: console,
            util: util,
            Buffer: Buffer,
            RED: {
                util: RED.util
            },
            __node__: {

                send: function(id, msgs) {
                    sendResults(node, id, msgs);
                }

            }

        };
        var context = vm.createContext(sandbox);
        try {
            this.script = vm.createScript(functionText);
            this.on("input", function(msg) {
                try {
                    //var start = process.hrtime();
                    context.msg = msg;
                    this.script.runInContext(context);
                    sendResults(this, msg._msgid, context.results);


                } catch (err) {


                }
            });
            this.on("close", function() {
                while (node.outstandingTimers.length > 0) {
                    clearTimeout(node.outstandingTimers.pop())
                }
                while (node.outstandingIntervals.length > 0) {
                    clearInterval(node.outstandingIntervals.pop())
                }
            })
        } catch (err) {
            // eg SyntaxError - which v8 doesn't include line number information
            // so we can't do better than this
            this.error(err);
        }
    }

    RED.nodes.registerType("sofia2-kp-insert", Sofia2SIB);
    RED.library.register("functions");
}
