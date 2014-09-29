var expect = require('chai').expect;
var _ = require('lodash');

var amqp = require('../');

describe('sz-amqp', function () {
  it('should export', function () {
    expect(amqp).to.be.an('object');
    expect(amqp).to.have.property('publisher');
    expect(amqp.publisher).to.be.a('function');
    expect(amqp).to.have.property('consumer');
    expect(amqp.consumer).to.be.a('function');
    expect(amqp).to.have.property('amqplib');
    expect(amqp.amqplib).to.be.an('object');
  });
});

describe('consumer', function () {
  it('should initialize', function () {
    var consumer = new amqp.consumer();

    expect(consumer).to.be.an('object');
    expect(consumer).to.have.property('init');
    expect(consumer.init).to.be.a('function');
    expect(consumer).to.have.property('start');
    expect(consumer.start).to.be.a('function');
    expect(consumer).to.have.property('stop');
    expect(consumer.stop).to.be.a('function');
    expect(consumer).to.have.property('getUrl');
    expect(consumer.getUrl).to.be.a('function');
    expect(consumer.getUrl()).to.equal('amqp://guest:guest@127.0.0.1:5672/%2F');
    expect(consumer).to.have.property('connect');
    expect(consumer.connect).to.be.a('function');
    expect(consumer).to.have.property('handleError');
    expect(consumer.handleError).to.be.a('function');
    expect(consumer).to.have.property('bindQueue');
    expect(consumer.bindQueue).to.be.a('function');
    expect(consumer).to.have.property('handleMessage');
    expect(consumer.handleMessage).to.be.a('function');
    expect(consumer).to.have.property('setMessageHandler');
    expect(consumer.setMessageHandler).to.be.a('function');
  });

  it('should connect/disconnect to/from amqp server', function (done) {
    var consumer = new amqp.consumer();

    consumer.start(function () {
      consumer.stop(function (err) {
        done(err);
      });
    });
  });

  it('should emit the ready event', function (done) {
    var consumer = new amqp.consumer();

    consumer.on('ready', function () {
      consumer.stop(done);
    });

    consumer.start();
  });
});

describe('publisher', function () {
  it('should initialize', function () {
    var publisher = new amqp.publisher();

    expect(publisher).to.be.an('object');
    expect(publisher).to.have.property('close');
    expect(publisher.close).to.be.a('function');
    expect(publisher).to.have.property('getUrl');
    expect(publisher.getUrl).to.be.a('function');
    expect(publisher.getUrl()).to.equal('amqp://guest:guest@127.0.0.1:5672/%2F');
    expect(publisher).to.have.property('connect');
    expect(publisher.connect).to.be.a('function');
    expect(publisher).to.have.property('handleError');
    expect(publisher.handleError).to.be.a('function');
    expect(publisher).to.have.property('bindExchange');
    expect(publisher.bindExchange).to.be.a('function');
    expect(publisher).to.have.property('publish');
    expect(publisher.publish).to.be.a('function');
    expect(publisher).to.have.property('drainQueue');
    expect(publisher.drainQueue).to.be.a('function');
  });

  it('should queue messages when not connected', function () {
    var publisher = new amqp.publisher();

    publisher.publish('test');

    expect(publisher._queue.length).to.equal(1);
    expect(publisher._drainTimeout).to.not.equal(null);

    clearTimeout(publisher._drainTimeout);
  });

  it('should connect/disconnect to/from amqp server', function (done) {
    var publisher = new amqp.publisher();

    publisher.connect(function () {
      publisher.close(done);
    });
  });

  it('should emit the ready event', function (done) {
    var publisher = new amqp.publisher();

    publisher.on('ready', function () {
      publisher.close(done);
    });

    publisher.connect();
  });

  it('should publish a message', function (done) {
    var publisher = new amqp.publisher();

    publisher.connect(function () {
      publisher.publish('test', function (err) {
        // this should never be called (only for confirm channels)
        expect(true).to.equal(false);
      });

      expect(publisher._queue.length).to.equal(0);

      publisher.close(done);
    });
  });

  it('should publish a message (confirm channel)', function (done) {
    var publisher = new amqp.publisher({confirmChannel: true});

    publisher.connect(function () {
      publisher.publish('test', function (err, ok) {
        expect(err).to.equal(null);
        console.log(ok);
        publisher.close(done);
      });

      expect(publisher._queue.length).to.equal(0);
    });
  });
});

describe('message patterns', function () {
  it('direct', function (done) {
    var publisher = new amqp.publisher();

    var consumer = new amqp.consumer(function (content, channel, message) {
      expect(content).to.equal('test');

      expect(channel).to.be.an('object');

      expect(message).to.be.an('object');
      expect(message).to.have.property('fields');
      expect(message.fields).to.be.an('object');
      expect(message).to.have.property('properties');
      expect(message.properties).to.be.an('object');
      expect(message).to.have.property('content');
      expect(message.content).to.be.an.instanceOf(Buffer);

      publisher.close(function () {
        consumer.stop(done);
      });
    });

    consumer.start(function () {
      publisher.connect(function () {
        publisher.publish('test');
      });
    });
  });

  it('pub/sub', function (done) {
    var publisher = new amqp.publisher();
    var messageCount = 0;

    var handler = function (content, channel, message) {
      messageCount++;

      expect(content).to.equal('test');

      expect(channel).to.be.an('object');

      expect(message).to.be.an('object');
      expect(message).to.have.property('fields');
      expect(message.fields).to.be.an('object');
      expect(message).to.have.property('properties');
      expect(message.properties).to.be.an('object');
      expect(message).to.have.property('content');
      expect(message.content).to.be.an.instanceOf(Buffer);

      if (messageCount == 2) {
        publisher.close(function () {
          consumer.stop(function () {
            consumer2.stop(done);
          });
        });
      }
    };

    var consumer = new amqp.consumer(handler);
    var consumer2 = new amqp.consumer(handler);

    consumer.start(function () {
      consumer2.start(function () {
        publisher.connect(function () {
          publisher.publish('test');
        });
      });
    });
  });

  it('load balance', function (done) {
    var publisher = new amqp.publisher();
    var messageCount = 0;

    var handler = function (content, channel, message) {
      messageCount++;

      expect(content).to.equal('test' + messageCount);

      expect(channel).to.be.an('object');

      expect(message).to.be.an('object');
      expect(message).to.have.property('fields');
      expect(message.fields).to.be.an('object');
      expect(message).to.have.property('properties');
      expect(message.properties).to.be.an('object');
      expect(message).to.have.property('content');
      expect(message.content).to.be.an.instanceOf(Buffer);

      expect(messageCount).to.be.at.most(2);

      if (messageCount == 2) {
        setTimeout(function () {
          publisher.close(function () {
            consumer.stop(function () {
              consumer2.stop(done);
            });
          });
        }, 50);
      }
    };

    var consumer = new amqp.consumer({queue: {name: 'test', options: {exclusive: false}}}, handler);
    var consumer2 = new amqp.consumer({queue: {name: 'test', options: {exclusive: false}}}, handler);

    consumer.start(function () {
      consumer2.start(function () {
        publisher.connect(function () {
          publisher.publish('test1');
          publisher.publish('test2');
        });
      });
    });
  });
});