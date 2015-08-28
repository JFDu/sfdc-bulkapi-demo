# Setup #

You'll need the following jars for the project. They are included in the source but here is a detailed explanation.

JDBC Driver for MySQL - download the latest MySQL driver from the MySQL site: http://www.mysql.com/downloads/connector/j/

Opencsv - a very simple CSV parser library for Java. This will allow us to easily write the MySQL result set to a CSV file. You can download it from the SourceForge project: http://sourceforge.net/projects/opencsv/

Force.com Web Service Connector (WSC) -  the WSC is a high performing web service client stack implemented using a streaming parser. The toolkit has built-in support for the basic operations and objects used in the Bulk API. You can download the latest version of the toolkit (wsc-17\_0.jar or wsc-18\_0.jar) from the download tab on the project site: http://code.google.com/p/sfdc-wsc/downloads/list

Partner Jar - since the Bulk API does not provide a login operation, we'll need to use the SOAP Web Services API with the Partner WSDL to login. Log into the target org and go to Setup -> Develop -> API. Right-click Partner WSDL to display your browser's save options, and save the partner WSDL to a local directory. Compile the partner API code using Partner WSDL and WSC compile tool:

> java -classpath pathToJar\wsc.jar com.sforce.ws.tools.wsdlc pathToWSDL\wsdlFilename .\wsdlGenFiles.jar


For example, if wsc.jar (the name may be different depending on the version name) is installed in C:\salesforce\wsc, and the partner WSDL is saved to C:\salesforce\wsdl\partner:

> java -classpath C:\salesforce\wsc\wsc.jar com.sforce.ws.tools.wsdlc C:\salesforce\wsdl\partner\partner.wsdl .\partner.jar


Now that we have these 4 jar files, we'll need to add them to our Java Build Path in Eclipse.