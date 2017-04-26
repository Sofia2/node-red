

// Sofia2 Notebook Node-RED node file


module.exports = function(RED) {
    "use strict";
	// require sofia2 properties
	var sofia2Config = require('./config/sofia2-config');
    // require any external libraries we may need....
	var zeppelinProtocol = sofia2Config.zeppelinProtocol;
	var zeppelinHost = sofia2Config.zeppelinHost;
	var zeppelinPort = sofia2Config.zeppelinPort;
	var zeppelinPath = sofia2Config.zeppelinPath;
	if(zeppelinProtocol=="https"){
		var http = require("https");
	}
	else{
		var http = require("http");
	}

	// require sofia2 properties
	var sofia2Config = require('./config/sofia2-config');

    // The main node definition - most things happen in here
    function Notebook(n) {
        // Create a RED node
        RED.nodes.createNode(this,n);

        this.topic = n.topic;

        // copy "this" object in case we need it in context of callbacks of other functions.
        var node = this;
		
		var queueExecution = [];
		var runnningExecution = false;
		var notebookID_Base=n.notebook;
		var notebookID_Clone;
		var notebookname=n.notebookname;
		var modeWorkflow=n.workflow;
		var checktime=n.checktime*1000;
		var nodeparameters = n.nodeparameters;
		var outputparameters = n.outputparameters;
		var paragraphSelector = n.nodeparagraphform;
		var fullexecutionnotebook = n.fullexecutionnotebook;
		var timeout = n.timeoutnotebook*1000;
		var enableoutputs = n.enableoutputs;
		var splitoutputs = n.splitoutputs;
		var nodeOutputIterator=0;
		var nodeOutputMsg=[];
		var nodeOutputHastToIndex={}
		
		var runNotebookPath = "/api/notebook/job/"
		var checkParagraphPath = "/api/notebook/"
		var cloneNotebookPath = "/api/notebook/"
		var deleteNotebookPath = "/api/notebook/"
		var getParagraphInfo = "/api/notebook/"
		var stopNotebookPath = "/api/notebook/job/"
		var checkNotebookPath = "/api/notebook/job/"
		
		//Interval/Timeouts vars
		var timeoutKillNotebook;
		var intervalID;
		
		//Get JSON Date NOW
		function nowJSON(){
			var auxDate = new Date();
			return auxDate.toJSON();
		}
		
		//Get notebook paragraph Output
		function getNotebookParagraphTempOutputAndSend(){
			unsetTimeoutNotebook();
			if(enableoutputs){
				node.warn("Trying to get paragraph output of " + notebookname);
				var outputParams = JSON.parse(outputparameters);
				
				for(var output=0;output<outputParams.length;output++){
					nodeOutputIterator++;
					nodeOutputMsg.push("");
				}
				for(var output=0;output<outputParams.length;output++){
					nodeOutputHastToIndex[outputParams[output].paragraph]=output;
					var runGetNotebookParagraphOutputOpts = {
						host: zeppelinHost,
						port: zeppelinPort,
						path: zeppelinPath + getParagraphInfo + notebookID_Clone + "/paragraph/" + outputParams[output].paragraph,
						method: 'GET'
					};
					var getNotebookParagraphOutputStatus_req = http.request(runGetNotebookParagraphOutputOpts, function(res) {
						switch(res.statusCode){
							case 200:
								res.on('data', function(chunk){
									var paragraphData = JSON.parse(chunk);
									nodeOutputMsg[nodeOutputHastToIndex[paragraphData.body.id]]={
										payload:paragraphData.body.result.msg,
										topic:outputParams[nodeOutputHastToIndex[paragraphData.body.id]].topic,
										type:paragraphData.body.result.type
									}
									nodeOutputIterator--;
									//todos los rest completados
									if(nodeOutputIterator==0){
										node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Notebook execution completed");
										//Send bungle msg (no split into outputs) in standard [[]] 
										if(!splitoutputs){
											nodeOutputMsg = [nodeOutputMsg];
										}
										node.send(nodeOutputMsg);
										deleteCloneNotebook();
									}
									
								});
								break;
							default:
								node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Can't get output of paragraph, execution stopped. Error is the following: " + res.statusMessage)
								deleteCloneNotebook();
								break;
						}
					});
					getNotebookParagraphOutputStatus_req.end();
				}
			}
			else{
				node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " +"No notebook output");
				node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Notebook execution completed");
				node.send(notebookID_Base);
				deleteCloneNotebook();
			}
		}
		
		//Clone notebook and get new ID
		function cloneNotebookAndRunTemp(){
			node.warn("Trying to clone Notebook " + notebookname);
			var runCloneNotebookOpts = {
				host: zeppelinHost,
				port: zeppelinPort,
				path: zeppelinPath + cloneNotebookPath + notebookID_Base,
				method: 'POST',
				json: true,
				headers: {
					"Content-Type": "application/json"
				}
			};
			var cloneStatus_req = http.request(runCloneNotebookOpts, function(res) {
				switch(res.statusCode){
					case 201:
						res.on('data', function(chunk){
							var responseJSON = JSON.parse(chunk);
							notebookID_Clone = responseJSON.body;
							node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Notebook " + notebookID_Clone + " created succesfully, copy of " + notebookname);
							//Running copy
							startCloneNotebook();
						});
						break;
					default:
						node.error("Notebook " + notebookname + " can't be clone. Error is the following: " + res.statusMessage)
						break;
				}
			});
			cloneStatus_req.end(JSON.stringify({"name": notebookID_Base + "_temp_" + nowJSON()}));
		}
		
		//Set timeout notebook
		function setTimeoutNotebook(){
			node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Set timeout notebook to " + (timeout/1000) + " seconds");
			timeoutKillNotebook = setTimeout(function(){
				clearInterval(intervalID);
				node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Notebook timeout reached");
				node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Trying to stop temporary Notebook");
				var runStopNotebookOpts = {
					host: zeppelinHost,
					port: zeppelinPort,
					path: zeppelinPath + stopNotebookPath + notebookID_Clone,
					method: 'DELETE'
				};
				var stopStatus_req = http.request(runStopNotebookOpts, function(res) {
					switch(res.statusCode){
						case 200:
							res.on('data', function(chunk){
								node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Stopped succesfully, deleting...");
								deleteCloneNotebook();
							});
							break;
						default:
							node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Can't be stop. Error is the following: " + res.statusMessage)
							node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Deleting temporary Notebook");
							deleteCloneNotebook();
							break;
					}
				});
				stopStatus_req.end();
			},timeout);
		}
		
		//Unset timeout notebook
		function unsetTimeoutNotebook(){
			node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "UnSet timeout notebook");
			clearTimeout(timeoutKillNotebook);
		}
		
		//Delete clone notebook
		function deleteCloneNotebook(){
			node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Trying to delete Notebook");
			var runDeleteNotebookOpts = {
				host: zeppelinHost,
				port: zeppelinPort,
				path: zeppelinPath + deleteNotebookPath + notebookID_Clone,
				method: 'DELETE'
			};
			var deleteStatus_req = http.request(runDeleteNotebookOpts, function(res) {
				switch(res.statusCode){
					case 200:
						res.on('data', function(chunk){
							node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Delete succesfully");
							//Si hay ejecuciones en espera se lanzan
							if(queueExecution.length==0){
								runnningExecution=false;
							}
							else{
								node.warn("Pending executions " + queueExecution.length);
								node.warn("Launching next " + queueExecution[0]);
								queueExecution.shift();
								cloneNotebookAndRunTemp()
							}
						});
						break;
					default:
						node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Can't be delete. Error is the following: " + res.statusMessage)
						if(queueExecution.length==0){
							runnningExecution=false;
						}
						else{
							node.warn("Pending executions after execution failed" + queueExecution.length);
							node.warn("Launching next " + queueExecution[0]);
							queueExecution.shift();
							cloneNotebookAndRunTemp()
						}
						break;
				}
			});
			deleteStatus_req.end();
		}
		
		//Start Clone Notebook
		function startCloneNotebook(){
			//Inicio de timeout
			setTimeoutNotebook();
			//Si hay párrafo de entrada
			if(paragraphSelector!=""){
				var runPostOpts = {
					  host: zeppelinHost,
					  port: zeppelinPort,
					  path: zeppelinPath + runNotebookPath + notebookID_Clone + "/" + paragraphSelector,
					  method: 'POST',
					  json: true,
					  headers: {
						"Content-Type": "application/json"
					  }
				};
				var post_req = http.request(runPostOpts, function(res) {
					switch(res.statusCode){
						case 200:
							node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Paragraph " + paragraphSelector + " started.");
							startCheckParagraphFinish(notebookID_Clone,5000);
							break;
						case 404:
							node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " +"Paragraph " + paragraphSelector + " not found.")
							break;
						default:
							node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " +"Paragraph " + paragraphSelector + "can't be execute. Error is the following: " + res.statusMessage)
							break;
					}
				});
				var formInputZ = {}
				var formInputZ_Proc = {}
				formInputZ=JSON.parse(nodeparameters);
				formInputZ_Proc["params"]={};
				for(var paramIT in formInputZ){
					try {
						if ( (formInputZ[paramIT].type == null && formInputZ[paramIT].value === "") || formInputZ[paramIT].type === "date") {
							formInputZ_Proc["params"][paramIT] = Date.now();
						} else if (formInputZ[paramIT].type == null) {
							formInputZ_Proc["params"][paramIT] = formInputZ[paramIT].value;
						} else if (formInputZ[paramIT].type == 'none') {
							formInputZ_Proc["params"][paramIT] = "";
						} else {
							formInputZ_Proc["params"][paramIT] = RED.util.evaluateNodeProperty(formInputZ[paramIT].value,formInputZ[paramIT].type,node,node.data[0][0]);
						}
						node.data.shift();
					} catch(err) {
						node.error(err,msg);
					}
				}
				node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): Sending " + JSON.stringify(formInputZ_Proc))
				post_req.end(JSON.stringify(formInputZ_Proc));
			}
			else{
				if(fullexecutionnotebook){
					node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Launching...");
					var runPostOpts = {
					  host: zeppelinHost,
					  port: zeppelinPort,
					  path: zeppelinPath + runNotebookPath + notebookID_Clone,
					  method: 'POST'
					};
					var post_req = http.request(runPostOpts, function(res) {
						switch(res.statusCode){
							case 200:
								node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + " started")
								startCheckNotebookFinish(notebookID_Clone);
								break;
							case 404:
								node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + " Not found notebook")
								break;
							default:
								node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Can't be execute. Error is the following: " + res.statusMessage)
								break;
						}
					});
					post_req.end();
				}
				else{
					//JSON to OUTPUT
					getNotebookParagraphTempOutputAndSend();
				}
			}
		}
		
		//check paragraph status
		function startCheckParagraphFinish(){
			node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Paragraph " + paragraphSelector + " started");
			intervalID = setInterval(function(){
				var runGETStatusOpts = {
					host: zeppelinHost,
					port: zeppelinPort,
					path: zeppelinPath + checkParagraphPath + notebookID_Clone + "/paragraph/" + paragraphSelector,
					method: 'GET'
				};
				var getStatus_req = http.request(runGETStatusOpts, function(res) {
					switch(res.statusCode){
						case 200:
							res.on('data', function(chunk){
								var status = JSON.parse(chunk);
								if(status.body.status!='FINISHED'){
									node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Paragraph " + paragraphSelector + " is still running");
									if(status.body.status=='ERROR'){
										clearInterval(intervalID);
										node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Paragraph " + paragraphSelector + " found and error --> " + status.body.result.msg + ". Execution stopped");
										deleteCloneNotebook();
									}
									return;
								}
								if(fullexecutionnotebook){
									node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Paragraph " + paragraphSelector + " finished succesfully, launching Notebook");
									var runPostOpts = {
									  host: zeppelinHost,
									  port: zeppelinPort,
									  path: zeppelinPath + runNotebookPath + notebookID_Clone,
									  method: 'POST'
									};
									var post_req = http.request(runPostOpts, function(res) {
										switch(res.statusCode){
											case 200:
												node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Started")
												startCheckNotebookFinish(notebookID_Clone);
												break;
											case 404:
												node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Notebook " + notebookID_Clone + " not found")
												break;
											default:
												node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Notebook " + notebookID_Clone + " can't be execute. Error is the following: " + res.statusMessage);
												break;
										}
									});
									post_req.end();
								}
								else{
									getNotebookParagraphTempOutputAndSend();
									node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " +"Paragraph " + notebookID_Clone + " finished succesfully, execution finished");
								}
								clearInterval(intervalID);
							});
							break;
						default:
							node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + " paragraph " + paragraphSelector + " can't be execute. Error is the following: Code:" + res.statusCode + ", " + res.statusMessage)
							break;
							
					}
				});
				getStatus_req.end();
			},checktime)
		}
		
		function getParagraphErrorAndDeleteTemp(paragraphError){
			var runGETStatusOpts = {
				host: zeppelinHost,
				port: zeppelinPort,
				path: zeppelinPath + checkParagraphPath + notebookID_Clone + "/paragraph/" + paragraphError,
				method: 'GET'
			};
			var getStatus_req = http.request(runGETStatusOpts, function(res) {
				switch(res.statusCode){
					case 200:
						res.on('data', function(chunk){
							var status = JSON.parse(chunk);
							node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "The details of the error were the following:" + "\n\nText: " + status.body.text + " \nStarted: " + status.body.dateStarted + " \nFinished: " + status.body.dateFinished + " \n\nError: " + status.body.result.msg)
							deleteCloneNotebook();
							unsetTimeoutNotebook();
						});
						break;
					default:
						node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Error paragraph " + paragraphSelector + " can't be found. Error is the following: Code:" + res.statusCode + ", " + res.statusMessage)
						deleteCloneNotebook();
						unsetTimeoutNotebook();
						break;
						
				}
			});
			getStatus_req.end();
		}
		
		//check notebook status
		function startCheckNotebookFinish(notebookID){
			node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Started");
			intervalID = setInterval(function(){
				var runGETStatusOpts = {
					host: zeppelinHost,
					port: zeppelinPort,
					path: zeppelinPath + runNotebookPath + notebookID,
					method: 'GET'
				};
				var getStatus_req = http.request(runGETStatusOpts, function(res) {
					switch(res.statusCode){
						case 200:
							res.on('data', function(chunk){
								var status = JSON.parse(chunk);
								for(var i=0;i<status.body.length;i++){
									if(status.body[i].status!='FINISHED'){
										node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + " Running: " + i + "/" + status.body.length);
										if(status.body[i].status=='ERROR'){
											clearInterval(intervalID);
											node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + " Found error in paragraph " + (i+1) + ". Execution stopped");
											getParagraphErrorAndDeleteTemp(status.body[i].id)
										}
										return;
									}
								}
								clearInterval(intervalID);
								node.warn("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + " Finished succesfully");
								getNotebookParagraphTempOutputAndSend();
							});
							break;
						default:
							node.error("Notebook " + notebookID_Clone+ "(" + notebookname + "): " + "Can't be execute. Error is the following: " + res.statusCode + ", " + res.statusMessage)
							break;
							
					}
				});
				getStatus_req.end();
			},checktime)
		}
		//Se inician los triggers
		node.warn("Recovering triggers of " + this.id);
		var notebookTriggers = node.context().flow.get('notebookTriggers');
		var lastDeployTime = node.context().flow.get('lastDeployTime')||0;
		var date = new Date();
		if(!notebookTriggers || date.getTime()-lastDeployTime>5000){
			var notebookTriggers={}
		}
		if(this.wires.length>0 && this.wires[0].length>0){
			for(var i=0;i<this.wires[0].length;i++){
				if(this.wires[0][i] in notebookTriggers){
					notebookTriggers[this.wires[0][i]]++;
				}
				else{
					notebookTriggers[this.wires[0][i]]=1;
				}
			}
			node.context().flow.set("notebookTriggers",notebookTriggers);
			node.context().flow.set('lastDeployTime',date.getTime());
		}
		node.nTriggers=0;
		node.data=[];
		node.msgs=[];
        
        this.on('input', function (msg) {
			if(modeWorkflow){
				var totalTriggers = ((node.context().flow.get("notebookTriggers")&&node.context().flow.get("notebookTriggers")[node.id])?node.context().flow.get("notebookTriggers")[node.id]:1);
				node.nTriggers++;
				node.msgs.push(msg);
				node.warn("Triggers " + node.nTriggers + " of " + (totalTriggers));
			}
			if(!modeWorkflow || node.nTriggers>=totalTriggers){
				node.data.push(node.msgs);
				node.nTriggers=0;
				node.msgs=[];
				if(!runnningExecution){
					runnningExecution=true;
					cloneNotebookAndRunTemp();
				}
				else{
					queueExecution.push(Date.now());
				}
			}
			return;
        });

        this.on("close", function() {
            
        });
    }

    RED.nodes.registerType("Notebook Launcher",Notebook);

}
