
module.exports = function(RED) {
	var Client = require('node-rest-client').Client;
	var sofia2Config = require('./config/sofia2-config');
	
	function LoadQuery(n) {
		RED.nodes.createNode(this,n);
		var node = this;
		var client = new Client();
		 
		var args = {
			requesConfig: { timeout: 1000 },
			responseConfig: { timeout: 2000 }
		};
		this.on('input', function(msg) {
			var ontology=msg.ontology;
			var targetDB=msg.targetDB;
			var queryType=msg.queryType;
			var query=msg.query;
			var url=msg.url;
			
			query = query.replace(/ /g, "+");
			
			var endpoint = sofia2Config.scriptBasePath + "/script/services/api_motorflujo/v01/Databases?ontology=" + ontology + "&targetDB=" + targetDB + "&queryType=" + queryType + "&query=" + query + "&user=" + n.user + "&password=" + n.password;
				
			var req = client.get(endpoint, args,function (data, response) {
				// parsed response body as js object 
				console.log("statusCode: ", response.statusCode);
				if(response.statusCode== 200){
					msg.ok=true;
				}else{
					msg.ok=false;
				}
				msg.payload=data;
				msg=JSON.stringify(msg);
				msg = JSON.parse(msg);
				node.send(msg);
			});
				req.on('requestTimeout', function (req) {
					msg.ok=false;
					console.log("request has expired");
					req.abort();
				});
				 
				req.on('responseTimeout', function (res) {
					msg.ok=false;
					console.log("response has expired");
				});
			
		});
		
		 
	}
	 RED.nodes.registerType("sofia2-query-dinamic",LoadQuery);
}
