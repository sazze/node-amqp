/**
 * @author Craig Thayer <cthayer@sazze.com>
 * @copyright 2014 Sazze, Inc.
 */

var amqplib = require('amqplib');
var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var AURA_DEFINED = !_.isUndefined(global.aura);

var log = (!AURA_DEFINED || _.isUndefined(global.aura.log) ? {
  error: console.error,
  warn: console.warn,
  info: console.info,
  debug: _.noop,
  verbose: _.noop
} : global.aura.log);

var defaultOptions = {
  host: process.env.SZ_AMQP_HOST || '127.0.0.1',
  port: process.env.SZ_AMQP_PORT || 5672,
  user: process.env.SZ_AMQP_USER || 'guest',
  password: process.env.SZ_AMQP_PASSWORD || 'guest',
  vhost: process.env.SZ_AMQP_VHOST || '/',
  exchange: {
    name: process.env.SZ_AMQP_EXCHANGE || 'amqp.direct',
    type: process.env.SZ_AMQP_EXCHANGE_TYPE || 'direct',
    options: {}
  },
  routingKey: process.env.SZ_AMQP_ROUTING_KEY || 'amqp.direct',
  queue: {
    name: process.env.SZ_AMQP_QUEUE || '',
    options: {
      durable: process.env.SZ_AMQP_QUEUE_DURABLE == 'true' || false,
      exclusive: (process.env.SZ_AMQP_QUEUE_EXCLUSIVE ? process.env.SZ_AMQP_QUEUE_EXCLUSIVE == 'true' : true),
      autoDelete: (process.env.SZ_AMQP_QUEUE_AUTODELETE ? process.env.SZ_AMQP_QUEUE_AUTODELETE == 'true' : true)
    },
    bindOptions: {}
  },

  connection: {
    heartbeat: 1
  },
  consumeOptions: {
    noAck: true       // set this to false to ack message manually
  }
};

function Consumer(options, messageHandler) {
  EventEmitter.call(this);

  if (_.isFunction(options)) {
    messageHandler = options;
    options = {};
  }

  if (!_.isFunction(messageHandler)) {
    messageHandler = _.noop;
  }

  if (!_.isPlainObject(options)) {
    options = {};
  }

  this.options = _.merge(defaultOptions, options);
  this.processMessage = messageHandler;
}

util.inherits(Consumer, EventEmitter);

module.exports = Consumer;

Consumer.prototype.getUrl = function () {
  return 'amqp://' + this.options.user + ':' + this.options.password + '@' + this.options.host + ':' + this.options.port + '/' + encodeURIComponent(this.options.vhost);
};

Consumer.prototype.init = function () {
  this.connectWait = 0;
  this._conn = null;
};

Consumer.prototype.start = function (cb) {
  this.init();
  this.connect(cb);
};

Consumer.prototype.stop = function (cb) {
  if (!_.isFunction(cb)) {
    cb = _.noop;
  }

  if (!this._conn) {
    cb();
    return;
  }

  try {
    this._conn.close().then(function () {
      cb();
    });
  } catch (e) {
    if (!_.isUndefined(e.stackAtStateChange)) {
      log.error(e.stackAtStateChange);
    }

    cb(e);
  }
};

Consumer.prototype.connect = function (cb) {
  if (_.isFunction(cb)) {
    this.once('ready', cb);
  }

  log.debug('connecting to amqp server (' + this.getUrl() + ')');

  var open = amqplib.connect(this.getUrl(), this.options.connection);

  var self = this;

  open.then(function (conn) {
    self.connectWait = 0;

    self._conn = conn;

    log.info('Connected to ' + self.options.host);

    conn.on('error', self.handleError.bind(self));

    conn.on('close', function () {
      log.info('Connection closed');
    });

    function createChannel() {
      log.debug('creating channel');

      conn.createChannel().then(self.bindQueue.bind(self, self.options.routingKey), channelError);
    }

    function channelError(err) {
      log.error(err.stack || err.message || err);

      log.debug('re-creating channel');

      createChannel();
    }

    createChannel();
  }, function (err) {
    self.handleError.call(self, err, cb);
  });
};

Consumer.prototype.handleError = function (err, cb) {
  log.error(err.stack || err.message || err);

  this.connectWait = (this.connectWait >= 30000 ? this.connectWait : (this.connectWait < 2 ? this.connectWait + 1 : this.connectWait * this.connectWait));

  log.debug('waiting for ' + this.connectWait + ' ms before re-connecting');

  setTimeout(this.connect.bind(this, cb), this.connectWait);
};

Consumer.prototype.bindQueue = function (routingKey, channel) {
  channel.assertExchange(this.options.exchange.name, this.options.exchange.type, this.options.exchange.options);
  channel.assertQueue(this.options.queue.name, this.options.queue.options);

  log.info('Binding to: ' + routingKey);

  channel.bindQueue(this.options.queue.name, this.options.exchange.name, routingKey, this.options.queue.bindOptions);

  channel.consume(this.options.queue.name, function (message) {
    this.handleMessage.call(this, message, channel);
  }.bind(this), this.options.consumeOptions);

  this.emit('ready');
};

Consumer.prototype.handleMessage = function (message, channel) {
  this.processMessage(message.content.toString(), channel, message);
};

Consumer.prototype.setMessageHandler = function (callback) {
  if (!_.isFunction(callback)) {
    callback = _.noop;
  }

  this.processMessage = callback;
};