
var sofia2 = require("../red/sofia2/sofia2-eventlistener-server.js");


module.exports = function(RED) {
	var request = require('sync-request');
	var sofia2Config = require('./config/sofia2-config');
	
    var urlService = sofia2Config.scriptBasePath + '/script/services/api_motorflujo/v01/ontologies';
	
    var listOntologiasUser = new Array();
    function obtenerOntologiasUsuario(user){
        debugger;
     console.log("entra en obtener ontologias");

    /*  var response = request('POST', urlService, {
			  json: { user: user }
			});


          
     if (response != null || response != undefined) {
                var response = JSON.parse(response.getBody('utf8'));

                console.log("RESPUESTA-------------->",response);

            }

      */ 
 
    }











    function SsapProcessRequest(config) {
		RED.nodes.createNode(this,config);
        var node = this;
        
              
        //obtenerOntologiasUsuario(user);



		sofia2.registerSsapProcessRequestEventListeners(this);
		
		
		this.on('close', function() {
			sofia2.deRegisterSsapProcessRequestEventListeners(this);
		});
		
		this.on('notifySofia2Event', function(event){
			var msgRaw = {payload: event};
			
			var msgJson = {payload: JSON.parse(event)};
			
			node.send([msgRaw, msgJson]);
		});
    }
    RED.nodes.registerType("ssap-process-request",SsapProcessRequest);
	
}
