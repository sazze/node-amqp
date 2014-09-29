/**
 * @author Craig Thayer <cthayer@sazze.com>
 * @copyright 2014 Sazze, Inc.
 */

module.exports = {
  consumer: require('./lib/consumer'),
  publisher: require('./lib/publisher'),
  amqplib: require('amqplib')
};