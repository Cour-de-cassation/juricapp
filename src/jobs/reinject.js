require('../modules/env');
const { Logger } = require('../modules/logger');
const logger = Logger.child({
  jobName: require('path').basename(__filename, '.js'),
});
const { Collector } = require('../modules/collector');

async function main() {
  logger.info('Start');
  let decisions;

  if (process.env.USE_SI_API === 'ON') {
    logger.info('Get decisions to reinject using DBSDER API');
    decisions = await Collector.getDecisionsToReinjectUsingAPI();
  } else {
    logger.info('Get decisions to reinject using direct DB access');
    decisions = await Collector.getDecisionsToReinjectUsingDB();
  }

  if (decisions && decisions.collected && Array.isArray(decisions.collected) && decisions.collected.length > 0) {
    logger.info(`${decisions.collected.length} decision(s) to reinject`);

    if (process.env.USE_DBSDER_API === 'ON') {
      logger.info('Reinject using SI API');
      await Collector.reinjectUsingAPI(decisions.collected);
    } else {
      logger.info('Reinject using direct DB access');
      await Collector.reinjectUsingDB(decisions.collected);
    }
  } else {
    logger.info('No decision to reinject');
  }

  logger.info('End');
  process.exit(0);
}

main();
