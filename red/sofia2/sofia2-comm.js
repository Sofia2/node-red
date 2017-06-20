var deployFlowsServicePath="/script/services/api_motorflujo/v01/Flujos";

//Pruebas PaaS
//var deployFlowsServicePath="/console/api/api_motorflujo/v01/Flujos";

var serverHost = 'localhost';

var serverPort = 9080;



var sofiaEventListener = require("./sofia2-eventlistener-server.js");
var http = require('http');

var domain;



//TODO Levantar aqui un unico servicio REST de comunicación con toda la infraestructura de Sofia2.
//Recibirá notificaciones a los nodos de este motor. Para eso se utiliza servicePortSofia2

module.exports = {
    log: function(trace) {
        console.log(trace);
    },
	setDomain: function(path){
		var pathArray=path.split( '/' );
		domain=pathArray[1];
	},
	setServicePort: function(thisServicePort){
		sofiaEventListener.init(thisServicePort);
	},
	notifyNodes: function(flows){
		var domainObject={'domain': domain};
		flows.unshift(domainObject);
		
		//TODO, aqui enviar al servicio REST del modulo Script
		//El módulo script filtrará los flujos y extraerá los nodos propios de Sofia2 con su configuracion
		
		var options = {
		  host: serverHost,
		  port: serverPort,
		  path: deployFlowsServicePath,
		  method: 'POST'
		};
		
		var postheaders = {
			'Content-Type' : 'application/json',
			'Content-Length' : Buffer.byteLength(flows, 'utf8')
		};
		 
		// do the POST call
		var reqPost = http.request(options, function(res) {
			res.on('data', function(d) {
				console.info('POST result:\n');
				process.stdout.write(d);
				console.info('\n\nPOST completed');
			});
		});
		reqPost.write(JSON.stringify(flows));
		reqPost.end();
		reqPost.on('error', function(e) {
			console.error(e);
		});
		
	//	console.log(domain);
	//	console.log(flows);
	},
	stop: function(){
		sofiaEventListener.stop();
	}
	
	
}