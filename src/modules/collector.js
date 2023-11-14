const { Database } = require('./database');
const { DateTime } = require('luxon');
const { Indexing } = require('./indexing');
const fs = require('fs');
const path = require('path');
const { Logger } = require('./logger');
const logger = Logger.child({
  moduleName: require('path').basename(__filename, '.js'),
});

class Collector {
  constructor() {}

  async collectNewDecisionsUsingDB() {
    let oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    oneMonthAgo.setHours(0, 0, 0, 0);
    let formattedOneMonthAgo = oneMonthAgo.getFullYear();
    formattedOneMonthAgo +=
      '-' + (oneMonthAgo.getMonth() + 1 < 10 ? '0' + (oneMonthAgo.getMonth() + 1) : oneMonthAgo.getMonth() + 1);
    formattedOneMonthAgo += '-' + (oneMonthAgo.getDate() < 10 ? '0' + oneMonthAgo.getDate() : oneMonthAgo.getDate());

    const decisions = await Database.find(
      'si.jurica',
      `SELECT *
        FROM JCA_DECISION
        WHERE JCA_DECISION.JDEC_HTML_SOURCE IS NOT NULL
        AND JCA_DECISION.HTMLA IS NULL
        AND JCA_DECISION.IND_ANO = 0
        AND JCA_DECISION.JDEC_DATE_CREATION >= '${formattedOneMonthAgo}'
        ORDER BY JCA_DECISION.JDEC_ID ASC`,
    );

    for (let i = 0; i < decisions.length; i++) {
      decisions[i] = await this.completeDecisionUsingDB(decisions[i]);
    }

    return await this.filterCollectedDecisionsUsingDB(decisions);
  }

  async getUpdatedDecisionsUsingDB(lastDate) {
    // (\w)\['(\w+)'\]
    // $1.$2
    const date = lastDate.toJSDate();
    let strDate = date.getFullYear();
    strDate += '-' + (date.getMonth() + 1 < 10 ? '0' + (date.getMonth() + 1) : date.getMonth() + 1);
    strDate += '-' + (date.getDate() < 10 ? '0' + date.getDate() : date.getDate());

    const decisions = await Database.find(
      'si.jurica',
      `SELECT *
      FROM JCA_DECISION
      WHERE JCA_DECISION.JDEC_HTML_SOURCE IS NOT NULL
      AND JCA_DECISION.JDEC_DATE_MAJ > '${strDate}'
      ORDER BY JCA_DECISION.JDEC_ID ASC`,
    );

    for (let i = 0; i < decisions.length; i++) {
      decisions[i] = await this.completeDecisionUsingDB(decisions[i]);
    }

    return await this.filterCollectedDecisionsUsingDB(decisions, true);
  }

  async completeDecisionUsingDB(decision) {
    // Extract "_portalis" ID (if any) from the document:
    try {
      let html = decision.JDEC_HTML_SOURCE;
      html = html.replace(/<\/?[^>]+(>|$)/gm, '');
      if (html && html.indexOf('Portalis') !== -1) {
        // Strict :
        let portalis = /Portalis(?:\s+|\n+)(\b\S{4}-\S-\S{3}-(?:\s?|\n+)\S+\b)/g.exec(html);
        if (portalis === null) {
          // Less strict :
          portalis =
            /Portalis(?:\s*|\n*):?(?:\s+|\n+)(\b\S{2,4}(?:\s*)-(?:\s*)\S(?:\s*)-(?:\s*)\S{3}(?:\s*)-(?:\s*)(?:\s?|\n+)\S+\b)/g.exec(
              html,
            );
          if (portalis === null) {
            // Even less strict :
            portalis =
              /Portalis(?:\s*|\n*):?(?:\s+|\n+)(\b\S{2,4}(?:\s*)-(?:\s*)\S{3}(?:\s*)-(?:\s*)(?:\s?|\n+)\S+\b)/g.exec(
                html,
              );
          }
        }
        portalis = portalis[1].replace(/\s/g, '').trim();
        decision._portalis = portalis;
      } else {
        decision._portalis = null;
      }
    } catch (ignore) {
      decision._portalis = null;
    }

    // Inject "bloc_occultation" data (if any) into the document:
    try {
      let blocId = null;
      if (decision.JDEC_CODNAC) {
        const NACResult = await Database.find(
          'si.jurica',
          `SELECT *
          FROM JCA_NAC
          WHERE JCA_NAC.JNAC_F22CODE = :code`,
          [decision.JDEC_CODNAC],
        );
        if (NACResult && NACResult.length > 0) {
          const indexBloc = NACResult[0].JNAC_IND_BLOC;
          if (indexBloc) {
            const GRCOMResult = await Database.find(
              'si.com',
              `SELECT *
              FROM BLOCS_OCCULT_COMPL
              WHERE BLOCS_OCCULT_COMPL.ID_BLOC = :code`,
              [indexBloc],
            );
            if (GRCOMResult && GRCOMResult.length > 0) {
              blocId = GRCOMResult[0].ID_BLOC;
              for (let key in GRCOMResult[0]) {
                if (key !== 'ID_BLOC' && decision[key] === undefined) {
                  decision[key] = GRCOMResult[0][key];
                }
              }
            }
          }
        }
      }
      decision._bloc_occultation = blocId;
    } catch (ignore) {
      decision._bloc_occultation = null;
    }

    return decision;
  }

  async filterCollectedDecisionsUsingDB(decisions, updated) {
    let whitelist = [];

    try {
      whitelist = JSON.parse(
        fs.readFileSync(path.join(__dirname, '..', '..', 'settings', 'id_collect_whitelist.json')).toString(),
      );
    } catch (ignore) {}

    const filtered = {
      collected: [],
      rejected: [],
    };

    for (let i = 0; i < decisions.length; i++) {
      const decision = decisions[i];

      if (updated === true) {
        // @TODO XXX HERE 5
        const found = await Database.findOne('sder.rawJurica', { _id: decision.JDEC_ID });
        if (found === null) {
          filtered.collected.push({
            decision: decision,
            diff: null,
          });
        } else {
          const updatable = [
            'XML',
            'TYPE_ARRET',
            'JURIDICTION',
            'ID_CHAMBRE',
            'NUM_DECISION',
            'DT_DECISION',
            'ID_SOLUTION',
            'TEXTE_VISE',
            'RAPROCHEMENT',
            'SOURCE',
            'DOCTRINE',
            'IND_ANO',
            'AUT_ANO',
            'DT_ANO',
            'DT_MODIF',
            'DT_MODIF_ANO',
            'DT_ENVOI_DILA',
            '_titrage',
            '_analyse',
            '_partie',
            '_decatt',
            '_portalis',
            '_bloc_occultation',
            'IND_PM',
            'IND_ADRESSE',
            'IND_DT_NAISSANCE',
            'IND_DT_DECE',
            'IND_DT_MARIAGE',
            'IND_IMMATRICULATION',
            'IND_CADASTRE',
            'IND_CHAINE',
            'IND_COORDONNEE_ELECTRONIQUE',
            'IND_PRENOM_PROFESSIONEL',
            'IND_NOM_PROFESSIONEL',
            'IND_BULLETIN',
            'IND_RAPPORT',
            'IND_LETTRE',
            'IND_COMMUNIQUE',
            'ID_FORMATION',
            'OCCULTATION_SUPPLEMENTAIRE',
            '_natureAffaireCivil',
            '_natureAffairePenal',
            '_codeMatiereCivil',
            '_nao_code',
          ];
          const shouldNotBeUpdated = ['XML'];
          const triggerReprocess = [
            'IND_PM',
            'IND_ADRESSE',
            'IND_DT_NAISSANCE',
            'IND_DT_DECE',
            'IND_DT_MARIAGE',
            'IND_IMMATRICULATION',
            'IND_CADASTRE',
            'IND_CHAINE',
            'IND_COORDONNEE_ELECTRONIQUE',
            'IND_PRENOM_PROFESSIONEL',
            'IND_NOM_PROFESSIONEL',
            'OCCULTATION_SUPPLEMENTAIRE',
            '_bloc_occultation',
            '_natureAffaireCivil',
            '_natureAffairePenal',
            '_codeMatiereCivil',
            '_nao_code',
          ];
          const sensitive = ['XML', '_partie', 'OCCULTATION_SUPPLEMENTAIRE'];
          let diff = null;
          let anomaly = false;
          let reprocess = false;
          updatable.forEach((key) => {
            if (JSON.stringify(decision[key]) !== JSON.stringify(found[key])) {
              if (diff === null) {
                diff = {};
              }
              if (sensitive.indexOf(key) !== -1) {
                diff[key] = {
                  old: '[SENSITIVE]',
                  new: '[SENSITIVE]',
                };
              } else {
                diff[key] = {
                  old: JSON.stringify(found[key]),
                  new: JSON.stringify(decision[key]),
                };
              }
              if (shouldNotBeUpdated.indexOf(key) !== -1) {
                anomaly = true;
              }
              if (triggerReprocess.indexOf(key) !== -1) {
                reprocess = true;
              }
            }
          });
          if (diff === null) {
            filtered.rejected.push({
              decision: decision,
              reason: 'decision has no significant difference',
            });
          } else {
            filtered.collected.push({
              decision: decision,
              diff: diff,
              anomaly: anomaly,
              reprocess: reprocess,
            });
          }
        }
      } else {
        try {
          let inDate = new Date();
          let dateDecisionElements = decision.JDEC_DATE.split('-');
          inDate.setFullYear(parseInt(dateDecisionElements[0], 10));
          inDate.setMonth(parseInt(dateDecisionElements[1], 10) - 1);
          inDate.setDate(parseInt(dateDecisionElements[2], 10));
          inDate.setHours(0);
          inDate.setMinutes(0);
          inDate.setSeconds(0);
          inDate.setMilliseconds(0);
          inDate = DateTime.fromJSDate(inDate);
          if (whitelist.indexOf(decision.JDEC_ID) === -1 && inDate.diffNow('months').toObject().months <= -6) {
            filtered.rejected.push({
              decision: decision,
              reason: 'decision is too old',
            });
          } else if (whitelist.indexOf(decision.JDEC_ID) === -1 && inDate.diffNow('days').toObject().days > 1) {
            filtered.rejected.push({
              decision: decision,
              reason: 'decision is too early',
            });
          } else {
            const found = await Database.findOne('sder.rawJurica', { _id: decision.JDEC_ID });
            if (whitelist.indexOf(decision.JDEC_ID) !== -1 || found === null) {
              filtered.collected.push({
                decision: decision,
              });
            } else {
              filtered.rejected.push({
                decision: decision,
                reason: 'decision already collected',
              });
            }
          }
        } catch (e) {
          filtered.rejected.push({
            decision: decision,
            reason: e.message,
          });
        }
      }
    }
    return filtered;
  }

  async storeAndNormalizeNewDecisionsUsingDB(decisions, updated) {
    for (let i = 0; i < decisions.length; i++) {
      let decision = decisions[i].decision;
      try {
        decision._indexed = null;
        if (updated === true) {
          if (decisions[i].diff === null) {
            await Database.insertOne('sder.rawJurica', decision);
            await Indexing.indexDecision('ca', decision, null, 'import in rawJurica (sync)');
          } else {
            if (decisions[i].reprocess === true) {
              decision.IND_ANO = 0;
              decision.HTMLA = null;
              if (decisions[i].anomaly === true) {
                await Indexing.updateDecision(
                  'ca',
                  decision,
                  null,
                  `update in rawJurica and reprocessed (sync) - original text could have been changed - changelog: ${JSON.stringify(
                    decisions[i].diff,
                  )}`,
                );
              } else {
                await Indexing.updateDecision(
                  'ca',
                  decision,
                  null,
                  `update in rawJurica and reprocessed (sync) - changelog: ${JSON.stringify(decisions[i].diff)}`,
                );
              }
            } else if (decisions[i].anomaly === true) {
              await Indexing.updateDecision(
                'ca',
                decision,
                null,
                `update in rawJurica (sync) - original text could have been changed - changelog: ${JSON.stringify(
                  decisions[i].diff,
                )}`,
              );
            } else {
              await Indexing.updateDecision(
                'ca',
                decision,
                null,
                `update in rawJurica (sync) - changelog: ${JSON.stringify(decisions[i].diff)}`,
              );
            }
            await Database.replaceOne('sder.rawJurica', { _id: decision._id }, decision);
          }
          await Indexing.indexAffaire('ca', decision);

          let normalized = await Database.findOne('sder.decisions', { sourceId: decision._id, sourceName: 'jurica' });
          if (normalized === null) {
            let normDec = (await Indexing.normalizeDecision('ca', decision, null, false, true)).result;
            const insertResult = await Database.insertOne('sder.decisions', normDec);
            normDec._id = insertResult.insertedId;
            await Indexing.indexDecision('sder', normDec, null, 'import in decisions (sync)');
          } else if (normalized.locked === false && decisions[i].diff !== null) {
            let normDec = (await Indexing.normalizeDecision('ca', decision, normalized, false, true)).result;
            normDec.dateCreation = new Date().toISOString();
            normDec.zoning = null;
            if (decisions[i].reprocess) {
              normDec.pseudoText = undefined;
              normDec.pseudoStatus = 0;
              normDec.labelStatus = 'toBeTreated';
              normDec.labelTreatments = [];
            }
            await Database.replaceOne('sder.decisions', { _id: normalized._id }, normDec);
            normDec._id = normalized._id;
            if (decisions[i].reprocess === true) {
              await Indexing.updateDecision(
                'sder',
                normDec,
                null,
                `update in decisions and reprocessed (sync) - changelog: ${JSON.stringify(decisions[i].diff)}`,
              );
            } else {
              await Indexing.updateDecision(
                'sder',
                normDec,
                null,
                `update in decisions (sync) - changelog: ${JSON.stringify(decisions[i].diff)}`,
              );
            }
          }
        } else {
          const ShouldBeRejected = (
            await Indexing.shouldBeRejected(
              'ca',
              decision.JDEC_CODNAC,
              decision.JDEC_CODNACPART,
              decision.JDEC_IND_DEC_PUB,
            )
          ).result;
          if (ShouldBeRejected === false) {
            let partiallyPublic = false;
            try {
              partiallyPublic = (
                await Indexing.isPartiallyPublic(
                  'ca',
                  decision.JDEC_CODNAC,
                  decision.JDEC_CODNACPART,
                  decision.JDEC_IND_DEC_PUB,
                )
              ).result;
            } catch (ignore) {}
            if (partiallyPublic) {
              let trimmedText;
              let zoning;
              try {
                trimmedText = (await Indexing.cleanContent('ca', decision.JDEC_HTML_SOURCE)).result;
                trimmedText = trimmedText
                  .replace(/\*DEB[A-Z]*/gm, '')
                  .replace(/\*FIN[A-Z]*/gm, '')
                  .trim();
              } catch (e) {
                throw new Error(
                  `Cannot process partially-public decision ${
                    decision._id
                  } because its text is empty or invalid: ${JSON.stringify(
                    e,
                    e ? Object.getOwnPropertyNames(e) : null,
                  )}.`,
                );
              }
              try {
                zoning = (await Indexing.getZones(decision._id, 'ca', trimmedText)).result;
                if (!zoning || zoning.detail) {
                  throw new Error(
                    `Cannot process partially-public decision ${
                      decision._id
                    } because its zoning failed: ${JSON.stringify(
                      zoning,
                      zoning ? Object.getOwnPropertyNames(zoning) : null,
                    )}.`,
                  );
                }
              } catch (e) {
                throw new Error(
                  `Cannot process partially-public decision ${decision._id} because its zoning failed: ${JSON.stringify(
                    e,
                    e ? Object.getOwnPropertyNames(e) : null,
                  )}.`,
                );
              }
              if (!zoning.zones) {
                throw new Error(
                  `Cannot process partially-public decision ${decision._id} because it has no zone: ${JSON.stringify(
                    zoning,
                    zoning ? Object.getOwnPropertyNames(zoning) : null,
                  )}.`,
                );
              }
              if (!zoning.zones.introduction) {
                throw new Error(
                  `Cannot process partially-public decision ${
                    decision._id
                  } because it has no introduction: ${JSON.stringify(
                    zoning.zones,
                    zoning.zones ? Object.getOwnPropertyNames(zoning.zones) : null,
                  )}.`,
                );
              }
              if (!zoning.zones.dispositif) {
                throw new Error(
                  `Cannot process partially-public decision ${
                    decision._id
                  } because it has no dispositif: ${JSON.stringify(
                    zoning.zones,
                    zoning.zones ? Object.getOwnPropertyNames(zoning.zones) : null,
                  )}.`,
                );
              }
              let parts = [];
              if (Array.isArray(zoning.zones.introduction)) {
                for (let ii = 0; ii < zoning.zones.introduction.length; ii++) {
                  parts.push(
                    trimmedText
                      .substring(zoning.zones.introduction[ii].start, zoning.zones.introduction[ii].end)
                      .trim(),
                  );
                }
              } else {
                parts.push(
                  trimmedText.substring(zoning.zones.introduction.start, zoning.zones.introduction.end).trim(),
                );
              }
              if (Array.isArray(zoning.zones.dispositif)) {
                for (let ii = 0; ii < zoning.zones.dispositif.length; ii++) {
                  parts.push(
                    trimmedText.substring(zoning.zones.dispositif[ii].start, zoning.zones.dispositif[ii].end).trim(),
                  );
                }
              } else {
                parts.push(trimmedText.substring(zoning.zones.dispositif.start, zoning.zones.dispositif.end).trim());
              }
              decision.JDEC_HTML_SOURCE = parts.join('\n\n[...]\n\n');
            }
            await Database.insertOne('sder.rawJurica', decision);
            await Indexing.indexDecision('ca', decision, null, 'import in rawJurica');
            await Indexing.indexAffaire('ca', decision);
            const ShouldBeSentToJudifiltre = (
              await Indexing.shouldBeSentToJudifiltre(
                'ca',
                decision.JDEC_CODNAC,
                decision.JDEC_CODNACPART,
                decision.JDEC_IND_DEC_PUB,
              )
            ).result;
            if (ShouldBeSentToJudifiltre === true) {
              try {
                const judifiltreResult = (
                  await Indexing.sendToJudifiltre(
                    decision._id,
                    'jurica',
                    decision.JDEC_DATE,
                    decision.JDEC_CODE_JURIDICTION,
                    decision.JDEC_CODNAC + (decision.JDEC_CODNACPART ? '-' + decision.JDEC_CODNACPART : ''),
                    decision.JDEC_IND_DEC_PUB === null
                      ? 'unspecified'
                      : parseInt(`${decision.JDEC_IND_DEC_PUB}`, 10) === 1
                      ? 'public'
                      : 'notPublic',
                  )
                ).result;
                await Indexing.updateDecision(
                  'ca',
                  decision,
                  duplicateId,
                  `submitted to Judifiltre: ${JSON.stringify(judifiltreResult)}`,
                );
                await Database.writeQuery(
                  'si.jurica',
                  `UPDATE JCA_DECISION
                    SET IND_ANO = :pending
                    WHERE JDEC_ID = :id`,
                  [1, decision._id],
                );
              } catch (e) {
                logger.error(`Jurica import to Judifiltre error processing decision ${decision._id}`, e);
                await Indexing.updateDecision('ca', decision, duplicateId, null, e);
                await Database.writeQuery(
                  'si.jurica',
                  `UPDATE JCA_DECISION
                    SET IND_ANO = :erroneous
                    WHERE JDEC_ID = :id`,
                  [4, decision._id],
                );
              }
            } else {
              let normalized = await Database.findOne('sder.decisions', {
                sourceId: decision._id,
                sourceName: 'jurica',
              });
              if (normalized === null) {
                let normDec = (await Indexing.normalizeDecision('ca', decision, null, false, true)).result;
                const insertResult = await Database.insertOne('sder.decisions', normDec);
                normDec._id = insertResult.insertedId;
                await Indexing.indexDecision('sder', normDec, null, 'import in decisions');
              } else {
                logger.warn(
                  `Jurica import anomaly: decision ${decision._id} seems new but a related SDER record ${normalized._id} already exists.`,
                );
                await Indexing.updateDecision('sder', normalized, null, `SDER record ${normalized._id} already exists`);
              }
              await Database.writeQuery(
                'si.jurica',
                `UPDATE JCA_DECISION
                  SET IND_ANO = :pending
                  WHERE JDEC_ID = :id`,
                [1, decision._id],
              );
            }
          } else {
            logger.warn(`Jurica import reject decision ${decision._id}.`);
            await Indexing.updateDecision('ca', decision, null, 'non-public');
            await Database.writeQuery(
              'si.jurica',
              `UPDATE JCA_DECISION
                SET IND_ANO = :erroneous
                WHERE JDEC_ID = :id`,
              [4, decision._id],
            );
          }
        }
      } catch (e) {
        await Indexing.updateDecision('ca', decision, null, null, e);
        await Database.writeQuery(
          'si.jurica',
          `UPDATE JCA_DECISION
            SET IND_ANO = :erroneous
            WHERE JDEC_ID = :id`,
          [4, decision._id],
        );
        if (updated) {
          logger.error(
            `storeAndNormalizeDecisionsUsingDB error for decision ${decision._id} (sync) - changelog: ${JSON.stringify(
              decisions[i].diff,
            )}`,
            e,
          );
        } else {
          logger.error(`storeAndNormalizeDecisionsUsingDB error for decision ${decision._id} (collect)`, e);
        }
      }
    }
    return true;
  }

  async getDecisionsToReinjectUsingDB() {
    const decisions = {
      collected: [],
      rejected: [],
    };

    decisions.collected = await Database.find(
      'sder.decisions',
      { labelStatus: 'done', sourceName: 'jurica' },
      { allowDiskUse: true },
    );

    return decisions;
  }

  async reinjectUsingDB(decisions) {
    for (let i = 0; i < decisions.length; i++) {
      const decision = decisions[i];
      try {
        // 1. Get the original decision from Jurica:
        const sourceDecision = await Database.findOne(
          'si.jurica',
          `SELECT *
          FROM JCA_DECISION
          WHERE JCA_DECISION.JDEC_ID = :id`,
          [decision.sourceId],
        );
        if (sourceDecision) {
          const now = new Date();
          let dateForIndexing = now.getFullYear() + '-';
          dateForIndexing += (now.getMonth() < 9 ? '0' + (now.getMonth() + 1) : now.getMonth() + 1) + '-';
          dateForIndexing += now.getDate() < 10 ? '0' + now.getDate() : now.getDate();
          // 2. Update query:
          await Database.writeQuery(
            'si.jurica',
            `UPDATE JCA_DECISION
            SET IND_ANO=:ok,
            AUT_ANO=:label,
            DT_ANO=:datea,
            JDEC_DATE_MAJ=:dateb,
            DT_MODIF_ANO=:datec,
            DT_ENVOI_ABONNES=NULL
            WHERE JDEC_ID=:id`,
            [2, 'LABEL', now, dateForIndexing, now, decision.sourceId],
          );
        } else {
          throw new Error(`reinjectUsingDB: decision '${decision.sourceId}' not found.`);
        }
      } catch (e) {
        logger.error(`Jurica reinjection error processing decision ${decision._id}`, e);
        await Indexing.updateDecision('sder', decision, null, null, e);
      }
    }
    return true;
  }

  // @TODO
  async collectNewDecisionsUsingAPI() {
    const decisions = {
      collected: [],
      rejected: [],
    };
    return decisions;
  }

  // @TODO
  async getUpdatedDecisionsUsingAPI(lastDate) {
    const decisions = {
      collected: [],
      rejected: [],
    };
    return decisions;
  }

  // @TODO
  async storeAndNormalizeNewDecisionsUsingAPI(decisions, updated) {
    return true;
  }

  // @TODO
  async getDecisionsToReinjectUsingAPI() {
    const decisions = {
      collected: [],
      rejected: [],
    };
    return decisions;
  }

  // @TODO
  async reinjectUsingAPI(decisions) {
    return true;
  }
}

exports.Collector = new Collector();
