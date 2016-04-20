Usage
============================
```javascript
var amqp = require('@sazze/amqp');

var publisher = new amqp.publisher();

publisher.connect();

publisher.publish('test');
```

```javascript
var amqp = require('@sazze/amqp');

var consumer = new amqp.consumer(function (content, channel, message) {
  // recieved message
});

consumer.start();
```

Install
============================
``` npm install @sazze/amqp ```

Tests
============================
*******************************************************************
***                                                             ***
*** local AMQP server (i.e. rabbitmq) is necessary to run tests ***
***                                                             ***
*******************************************************************

``` npm test ```

Configuration for rabbitmq is provided in the `test/rabbitmq-config` folder.  Copy or link the contents to the configuration directory of your rabbitmq installation to run the tests.

The passwords for the `p12` files for the server and client certificates are `test-server` and `test-client` respectively.
