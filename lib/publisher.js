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

  connection: {
    heartbeat: 1
  },
  confirmChannel: false
};

function Publisher(options) {
  EventEmitter.call(this);

  if (!_.isPlainObject(options)) {
    options = {};
  }

  this.options = _.merge({}, defaultOptions, options);

  this.connectWait = 0;
  this._conn = null;
  this._channel = null;
}

util.inherits(Publisher, EventEmitter);

module.exports = Publisher;

Publisher.prototype.getUrl = function () {
  return 'amqp://' + this.options.user + ':' + this.options.password + '@' + this.options.host + ':' + this.options.port + '/' + encodeURIComponent(this.options.vhost);
};

Publisher.prototype.close = function (cb) {
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

Publisher.prototype.connect = function (cb) {
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

      if (self.options.confirmChannel) {
        conn.createConfirmChannel().then(self.bindExchange.bind(self), channelError);
        return;
      }

      conn.createChannel().then(self.bindExchange.bind(self), channelError);
    }

    function channelError(err) {
      this._channel = null;

      log.error(err.stack || err.message || err);

      log.debug('re-creating channel');

      createChannel();
    }

    createChannel();
  }, function (err) {
    self.handleError.call(self, err, cb);
  });
};

Publisher.prototype.handleError = function (err, cb) {
  this._channel = null;
  this._conn = null;

  log.error(err.stack || err.message || err);

  this.connectWait = (this.connectWait >= 30000 ? this.connectWait : (this.connectWait < 2 ? this.connectWait + 1 : this.connectWait * this.connectWait));

  log.debug('waiting for ' + this.connectWait + ' ms before re-connecting');

  setTimeout(this.connect.bind(this, cb), this.connectWait);
};

Publisher.prototype.bindExchange = function (channel) {
  log.info('Binding to: ' + this.options.exchange.name);

  this._channel = channel;

  channel.assertExchange(this.options.exchange.name, this.options.exchange.type, this.options.exchange.options);

  this.emit('ready');
};

/**
 *
 * @param {Buffer|string|object} message the message to publish
 * @param {object} [options] the options for the message (default: {})
 * @param {string} [routingKey] the routing key for the message (default: options.routingKey)
 * @param {function} [callback] will be called after message has been published, if options.confirmChannel = true
 */
Publisher.prototype.publish = function (message, options, routingKey, callback) {
  if (_.isFunction(options)) {
    callback = options;
    options = {};
  }

  if (_.isFunction(routingKey)) {
    callback = routingKey;
    routingKey = undefined;
  }

  if (!_.isPlainObject(options)) {
    options = {};
  }

  routingKey = routingKey || this.options.routingKey;

  if (!_.isFunction(callback)) {
    callback = _.noop;
  }

  // convert message to buffer
  var msgBuffer;

  if (Buffer.isBuffer(message)) {
    msgBuffer = message;
  } else if (_.isString(message)) {
    msgBuffer = new Buffer(message);
  } else {
    msgBuffer = new Buffer(JSON.stringify(message));
  }

  if (!this._channel) {
    this.once('ready', function () {
      this.publish(message, options, routingKey, callback);
    }.bind(this));

    return;
  }

  if (!this._channel.publish(this.options.exchange.name, routingKey, msgBuffer, options, callback)) {
    // channel write buffer is full, wait for "drain" event before writing more
    this.once('ready', function () {
      this.publish(message, options, routingKey, callback);
    }.bind(this));

    this._channel.once('drain', function () {
      this.emit('ready');
    }.bind(this));
  }
};