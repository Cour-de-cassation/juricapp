require('../modules/env');
const { Logger } = require('../modules/logger');
const logger = Logger.child({
  jobName: require('path').basename(__filename, '.js'),
});
const { Database } = require('../modules/database');
const { Indexing } = require('../modules/indexing');

async function main() {
  logger.info('Start');

  let row;
  let newCount = 0;
  let updateCount = 0;
  let deleteCount = 0;
  let skipCount = 0;
  let errorCount = 0;

  try {
    const batch = await Indexing.getPublicFromJudifiltre();
    if (batch && batch.releasableDecisions && Array.isArray(batch.releasableDecisions)) {
      for (let i = 0; i < batch.releasableDecisions.length; i++) {
        if (
          batch.releasableDecisions[i] &&
          batch.releasableDecisions[i].sourceId &&
          batch.releasableDecisions[i].sourceDb === 'jurica'
        ) {
          try {
            row = await Database.findOne('sder.rawJurica', { _id: batch.releasableDecisions[i].sourceId });
            if (row) {
              let normalized = await Database.findOne('sder.decisions', { sourceId: row._id, sourceName: 'jurica' });
              if (normalized === null) {
                let normDec = (await Indexing.normalizeDecision('ca', row, null, false, true)).result;
                normDec.public = true;
                const insertResult = await Database.insertOne('sder.decisions', normDec);
                normDec._id = insertResult.insertedId;
                await Indexing.indexDecision('sder', normDec, null, 'is-public, import in decisions');
                newCount++;
                try {
                  const judifiltreResult = await Indexing.deleteFromJudifiltre(
                    batch.releasableDecisions[i].sourceId,
                    batch.releasableDecisions[i].sourceDb,
                  );
                  await Indexing.updateDecision(
                    'ca',
                    row,
                    null,
                    `is-public, deleted from Judifiltre: ${JSON.stringify(judifiltreResult)}`,
                  );
                } catch (e) {
                  console.error(`Judifiltre delete public error`, e);
                  errorCount++;
                }
              } else {
                console.warn(
                  `Jurica import public issue: { sourceId: ${row._id}, sourceName: 'jurica' } already inserted...`,
                );
              }
            } else {
              let normDec = (await Indexing.normalizeDecision('ca', row, normalized, false, true)).result;
              normDec.public = true;
              normDec._id = normalized._id;
              await Database.replaceOne('sder.decisions', { _id: normDec._id }, normDec);
              await Indexing.indexDecision(normDec, null, 'is-public, update in decisions');
              updateCount++;
              try {
                const judifiltreResult = await Indexing.deleteFromJudifiltre(
                  batch.releasableDecisions[i].sourceId,
                  batch.releasableDecisions[i].sourceDb,
                );
                await Indexing.updateDecision(
                  'ca',
                  row,
                  null,
                  `is-public, deleted from Judifiltre: ${JSON.stringify(judifiltreResult)}`,
                );
              } catch (e) {
                console.error(`Judifiltre delete public error`, e);
                errorCount++;
              }
            }
          } catch (e) {
            console.error(`Judifiltre import public error`, batch.releasableDecisions[i]);
            errorCount++;
          }
        } else {
          console.log(`Judifiltre skip public decision`, batch.releasableDecisions[i]);
          skipCount++;
        }
      }
    } else {
      console.error(`Judifiltre import public error`, batch);
      errorCount++;
    }
  } catch (e) {
    console.error(`Judifiltre import public error`, e);
    errorCount++;
  }

  try {
    const batch = await Indexing.getNotPublicFromJudifiltre();
    if (batch && batch.notPublicDecisions && Array.isArray(batch.notPublicDecisions)) {
      for (let i = 0; i < batch.notPublicDecisions.length; i++) {
        if (
          batch.notPublicDecisions[i] &&
          batch.notPublicDecisions[i].sourceId &&
          batch.notPublicDecisions[i].sourceDb === 'jurica'
        ) {
          try {
            row = await Database.findOne('sder.rawJurica', { _id: batch.notPublicDecisions[i].sourceId });
            if (row) {
              let normalized = await Database.findOne('sder.decisions', { sourceId: row._id, sourceName: 'jurica' });
              if (normalized !== null) {
                await Database.deleteOne('sder.decisions', { _id: normalized._id });
              }

              await Database.deleteOne('sder.rawJurica', { _id: row._id });

              try {
                const judifiltreResult = await Indexing.deleteFromJudifiltre(
                  batch.notPublicDecisions[i].sourceId,
                  batch.notPublicDecisions[i].sourceDb,
                );
                await Indexing.updateDecision(
                  'ca',
                  row,
                  null,
                  `not-public, deleted from Judifiltre: ${JSON.stringify(judifiltreResult)}`,
                );
                deleteCount++;
              } catch (e) {
                console.error(`Judifiltre cleaning non-public error`, e);
                errorCount++;
              }
            }
          } catch (e) {
            console.error(`Judifiltre cleaning non-public error`, batch.notPublicDecisions[i]);
            errorCount++;
          }
        } else {
          console.log(`Judifiltre skip non-public decision`, batch.notPublicDecisions[i]);
          skipCount++;
        }
      }
    } else {
      console.error(`Judifiltre cleaning non-public error`, batch);
      errorCount++;
    }
  } catch (e) {
    console.error(`Judifiltre cleaning non-public error`, e);
    errorCount++;
  }

  console.log(
    `Done Importing/Cleaning Judifiltre - New: ${newCount}, Update: ${updateCount}, Cleaned: ${deleteCount}, Skip: ${skipCount}, Error: ${errorCount}.`,
  );

  logger.info('End');
  process.exit(0);
}

main();
