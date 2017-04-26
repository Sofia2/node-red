module.exports = function(RED) {
    "use strict";
    var util = require("util");
    var vm = require("vm");



    var name ="";
    var cipherKey = null;
    var kpName = "";
    var token = "";
    var thinKp = "";
    var instanciakp = "";
    var ontology = "";
    var query = "";
    var tipoQuery = "";
    
    var msRefresh = "";
    var pingQuery = "";
    var pingType = "";
    var pingTimer = "";
    
    var sessionKey = "";
    var data = "";
    var subscribeResponse = "";
    var queryResponse = "";
    var joinResponse = "";
    //Buffer de peticiones pendientes para envio por websocket
    var pendingWSRequestsBuffer = [];
    var node;




    /**
     * Crea el nodo SOFIA2-SIB
     * @author clobato
     */
    function Sofia2SIB(n) {
        RED.nodes.createNode(this, n);
        
        node = this;

        this.name = n.name;
        this.ontology = n.ontology;
        this.token = n.token;
        this.instanciakp = n.instanciakp;
        this.thinKp = n.thinKp;
        this.query = n.query;
        this.tipoQuery = n.tipoQuery;
        this.msRefresh = n.msRefresh;
        this.pingQuery = n.pingQuery;
        this.pingType = n.pingType;
        this.pingTimer = n.pingTimer;
        sendResults(this);
        
    }




    /**
     * Procesa los datos del nodo para enviar el mensaje
     * @author clobato
     */
    function sendResults(node) {
        console.log("Procesamos los datos del Nodo [sofia2-kp-subscribe]");

        if (node != null && node != undefined) {
            if(node.name !=undefined){
                name = node.name;
            }

            if (node.ontology != undefined) {
                ontology = node.ontology;
                
            }
            if (node.token != undefined) {
                token = node.token;
                
            }
            if(node.thinKp !=undefined){
                thinKp = node.thinKp;
            }
            if (node.instanciakp != undefined) {
                instanciakp = node.instanciakp;
                
            }
            if (node.query != undefined) {
                query = node.query;
               
            }

            if (node.tipoQuery != undefined) {
                tipoQuery = node.tipoQuery;
                //console.log("ontology: " + ontology);
            }

            
            if (node.msRefresh !=undefined){
                msRefresh = node.msRefresh;
                msRefresh = parseInt(msRefresh);
            }else{
                msRefresh = 100; 
            }  

            if (node.pingQuery !=undefined){
                pingQuery = node.pingQuery;
            }

            if (node.pingType !=undefined){
                pingType = node.pingType;
            }
           
            if (node.pingTimer !=undefined){
                pingTimer = node.pingTimer;
            }

            //JOIN al SIB
            
          
            generateJoinMessage(instanciakp,token);

        }




    }

    
    function generateJoinMessage(instanciakp,token){

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


    function generateQueryMessage(pingQuery, pingType,ontology,sessionKey){

         console.log("----generateQueryMessage----");


         var querySib = '{"body":"{\\"query\\":\\"' 
                + pingQuery
                + '\\",\\"queryType\\":\\"'
                + pingType+'\\",\\"queryParams\\": null}","direction":"REQUEST","ontology":' 
                + ontology
                + ',"messageType":"QUERY","messageId":null,"sessionKey":"'
                + sessionKey + '"}';

        sendMessage("QUERY",querySib,false,queryResponse);    

    }


    function generateSubscribeMessage (sessionKey, ontology, msRefresh, query, tipoQuery){

            var querySubscribe = '{"body":"{\\"query\\":\\"' + query
                + '\\",\\"msRefresh\\":\\"' + msRefresh
                + '\\",\\"queryType\\":\\"'+tipoQuery.toString().toUpperCase()+'\\"}","direction":"REQUEST","ontology":"' + ontology
                + '","messageType":"SUBSCRIBE","messageId":null,"sessionKey":"'
                + sessionKey + '"}';
                               
            sendMessage("SUBSCRIBE", querySubscribe, false, subscribeResponse);       

    }

    /**
     * Envia el mensaje construido al SIB de Sofia 2
     * @author clobato
     */
    function sendMessage(_tipoQuery, _query, _cipherMessage, _responseCallback) {

        var WebSocketClient = require('websocket').client;
        var client = new WebSocketClient();


        client.connect('ws://sofia2.com/sib/api_websocket');



        client.on('connectFailed', function(error) {
            console.log('Connect Error: ' + error.toString());
            console.log("");
        });

        client.on('connect', function(connection) {
            console.log('WebSocket Client Connected');
            console.log("");

            connection.on('error', function(error) {
                console.log("Connection Error: " + error.toString());
                console.log("");
            });
            connection.on('close', function() {
                console.log('echo-protocol Connection Closed');
                console.log("");
            });

            connection.on('message', function(message) {
                if (message.type === 'utf8') {

                    console.log("Received Message : '" + message.utf8Data + "'");
                    console.log("");
                    var messageSSAP = JSON.parse(message.utf8Data);

                    pendingWSRequestsBuffer.shift();



                    if (messageSSAP.messageType == "JOIN" && (messageSSAP.sessionKey != null || messageSSAP.sessionKey != undefined)) {
                        sessionKey = messageSSAP.sessionKey;

                        generateSubscribeMessage(sessionKey, ontology, msRefresh, query, tipoQuery);

                    }

                    if(messageSSAP.messageType =="INDICATION"){

                            var msgJson = {payload:messageSSAP};
                            node.send([msgJson]);
                           
                              
                             generateQueryMessage(pingQuery,pingType,ontology,sessionKey);

                    }

                    if(messageSSAP.messageType =="QUERY"){

                       setInterval(function () { 
                             connection.send(queryToSend);   
                          }, parseInt(pingTimer));

                    }else if(messageSSAP.messageType =="QUERY" && (messageSSAP.sessionKey==null || messageSSAP.sessionKey==undefined) ){

                        generateJoinMessage(instanciakp,token);
                    }


                }
            });
            

            if (connection.connected) {
                               
                pendingWSRequestsBuffer.push({
                    tipoQuery: _tipoQuery,
                    query: _query,
                    cipherMessage: _cipherMessage,
                    responseCallback: _responseCallback
                });


                
                if (pendingWSRequestsBuffer.length == 1) {
                    
                    var queryToSend = pendingWSRequestsBuffer[0].query;

                    if (pendingWSRequestsBuffer[0].cipherMessage) {
                        if (_tipoQuery == "JOIN") {
                            queryToSend = kpName.length + "#" + kpName + Base64.encode(XXTEA.encrypt(queryToSend, cipherKey), false);
                        }

                        
                    }

                    


                    console.log("Query to send: "+queryToSend + "\n");
                    connection.send(queryToSend);

             
                      
                        
             

                   

                }

            }

        });



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


    

    RED.nodes.registerType("sofia2-kp-subscribe", Sofia2SIB);
    
}
