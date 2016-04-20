var expect = require('chai').expect;
var _ = require('lodash');
var path = require('path');

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
  });

  it('should support TLS', function () {
    //
    // publisher
    //
    var publisher = new amqp.publisher({port: 5671, ssl: {enable: true}});

    expect(publisher.getUrl()).to.equal('amqps://guest:guest@127.0.0.1:5671/%2F?verify=verify_peer&fail_if_no_peer_cert=true');

    publisher = new amqp.publisher({port: 5671, ssl: {enable: true, cacertfile: '/path/to/ca/cert.pem', certfile: '/path/to/cert.pem', keyfile: '/path/to/key.pem'}});

    expect(publisher.getUrl()).to.equal('amqps://guest:guest@127.0.0.1:5671/%2F?verify=verify_peer&fail_if_no_peer_cert=true&cacertfile=%2Fpath%2Fto%2Fca%2Fcert.pem&certfile=%2Fpath%2Fto%2Fcert.pem&keyfile=%2Fpath%2Fto%2Fkey.pem');

    //
    // consumer
    //
    var consumer = new amqp.consumer({port: 5671, ssl: {enable: true}});

    expect(consumer.getUrl()).to.equal('amqps://guest:guest@127.0.0.1:5671/%2F?verify=verify_peer&fail_if_no_peer_cert=true');

    consumer = new amqp.consumer({port: 5671, ssl: {enable: true, cacertfile: '/path/to/ca/cert.pem', certfile: '/path/to/cert.pem', keyfile: '/path/to/key.pem'}});

    expect(consumer.getUrl()).to.equal('amqps://guest:guest@127.0.0.1:5671/%2F?verify=verify_peer&fail_if_no_peer_cert=true&cacertfile=%2Fpath%2Fto%2Fca%2Fcert.pem&certfile=%2Fpath%2Fto%2Fcert.pem&keyfile=%2Fpath%2Fto%2Fkey.pem');
  });

  it('should connect/disconnect to/from amqp server', function (done) {
    var publisher = new amqp.publisher();

    publisher.connect(function () {
      publisher.close(done);
    });
  });

  it('should connect/disconnect to/from amqp server using TLS', function (done) {
    // this is necessary for node to accept self-signed certs (ok for testing)
    var NODE_TLS_REJECT_UNAUTHORIZED = process.env.NODE_TLS_REJECT_UNAUTHORIZED;

    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

    var opts = {
      port: 5671,
      ssl: {
        enable: true
      }
    };

    var publisher = new amqp.publisher(opts);

    publisher.connect(function () {
      // set this environment variable back to what it was before we changed it
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = NODE_TLS_REJECT_UNAUTHORIZED;

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
        // there should never be an error without using a confirm channel
        expect(_.isUndefined(err)).to.equal(true);

        publisher.close(done);
      });
    });
  });

  it('should publish a message (confirm channel)', function (done) {
    var publisher = new amqp.publisher({confirmChannel: true});

    publisher.connect(function () {
      publisher.publish('test', function (err, ok) {
        expect(err).to.equal(null);

        publisher.close(done);
      });
    });
  });

  it('should queue messages when not connected', function (done) {
    var publisher = new amqp.publisher({confirmChannel: true});
    var messageCount = 0;

    publisher.publish('test1', function (err) {
      messageCount++;

      expect(messageCount).to.equal(1);
      expect(err).to.equal(null);
    });

    publisher.publish('test2', function (err) {
      messageCount++;

      expect(messageCount).to.equal(2);
      expect(err).to.equal(null);

      publisher.close(done);
    });

    publisher.connect();
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
        publisher.close(function () {
          consumer.stop(function () {
            consumer2.stop(done);
          });
        });
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