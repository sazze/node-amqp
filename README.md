Usage
============================
```javascript
var amqp = require('sz-amqp');

var publisher = new amqp.publisher();

publisher.connect();

publisher.publish('test');
```

```javascript
var amqp = require('sz-amqp');

var consumer = new amqp.consumer(function (content, channel, message) {
  // recieved message
});

consumer.start();
```

Install
============================
``` npm install sz-amqp ```

Tests
============================
*******************************************************************
***                                                             ***
*** local AMQP server (i.e. rabbitmq) is necessary to run tests ***
***                                                             ***
*******************************************************************

``` npm test ```