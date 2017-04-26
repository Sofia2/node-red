
var sofia2 = require("../red/sofia2/sofia2-eventlistener-server.js");

module.exports = function(RED) {
	
    function NotificadorReglas(n) {
		RED.nodes.createNode(this,n);
        var node = this;
		var topic = n.topic;
        
		sofia2.registerNotifyRulesEventListeners(this, topic);
		
		this.on('close', function() {
			sofia2.deRegisterNotifyRulesEventListeners(this, topic);
		});
		
		this.on('notifySofia2Event', function(event){
			console.log("Se manda el mensaje.");
			var msg = {payload: event};
			msg = JSON.stringify(msg);
			msg = JSON.parse(msg);
			node.send(msg);
		});
    }
    RED.nodes.registerType("script-topic",NotificadorReglas);
	
}
