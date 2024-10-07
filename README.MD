# Asterisk AMI Collector
## Collector for connecting one business application to several asterisk servers.

Sometimes a business application is designed with the ability to connect to only one asterisk. But there comes a moment when there are several asterisks, and the application is not designed for this. In this case, you can use this collector. It will collect AMI messages from all servers specified in the configuration and forward them to the business application.

In the opposite direction (actions from the business application to the asterisk), commands will be sent only to the asterisk server that has the action=true parameter in the configuration.

tested on python 3.12