require('../modules/env');
const { Logger } = require('../modules/logger');
const logger = Logger.child({
  jobName: require('path').basename(__filename, '.js'),
});
const { DateTime } = require('luxon');
const { Collector } = require('../modules/collector');

async function main() {
  logger.info('Start');

  let now = DateTime.now();
  let lastDate;

  try {
    lastDate = DateTime.fromISO(
      fs.readFileSync(path.join(__dirname, '..', '..', 'settings', 'update.lastUpdate')).toString(),
    );
  } catch (ignore) {
    lastDate = now.minus({ days: 2 });
  }

  let decisions;

  if (process.env.USE_SI_API === 'ON') {
    logger.info('Update using SI API');
    decisions = await Collector.getUpdatedDecisionsUsingAPI(lastDate);
  } else {
    logger.info('Update using direct DB access');
    decisions = await Collector.getUpdatedDecisionsUsingDB(lastDate);
  }

  if (decisions && decisions.collected && Array.isArray(decisions.collected) && decisions.collected.length > 0) {
    logger.info(`${decisions.collected.length} decision(s) updated`);

    if (process.env.USE_DBSDER_API === 'ON') {
      logger.info('Store and normalize using DBSDER API');
      await Collector.storeAndNormalizeNewDecisionsUsingDB(decisions.collected, true);
    } else {
      logger.info('Store and normalize using direct DB access');
      await Collector.storeAndNormalizeNewDecisionsUsingDB(decisions.collected, true);
    }

    for (let i = 0; i < decisions.collected.length; i++) {
      let decision = decisions.collected[i].decision;
      let modifTime = DateTime.fromJSDate(decision.DT_MODIF);
      lastDate = DateTime.max(lastDate, modifTime);
    }
  } else {
    logger.info('No decision updated');
  }

  try {
    fs.writeFileSync(path.join(__dirname, '..', '..', 'settings', 'update.lastUpdate'), lastDate.toISO());
  } catch (e) {
    logger.error(e);
  }

  logger.info('End');
  process.exit(0);
}

main();
