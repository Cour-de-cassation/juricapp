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
    // @TODO
    // (\w)\['(\w+)'\]
    // $1.$2
    const date = lastDate.toJSDate();
    let strDate = date.getDate() < 10 ? '0' + date.getDate() : date.getDate();
    strDate += '/' + (date.getMonth() + 1 < 10 ? '0' + (date.getMonth() + 1) : date.getMonth() + 1);
    strDate += '/' + date.getFullYear();

    const decisions = await Database.find(
      'si.jurinet',
      `SELECT *
      FROM DOCUMENT
      WHERE DOCUMENT.XML IS NOT NULL
      AND DOCUMENT.DT_MODIF > TO_DATE('${strDate}', 'DD/MM/YYYY')
      ORDER BY DOCUMENT.ID_DOCUMENT ASC`,
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
        // @TODO
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
          // @TODO
          if (decisions[i].diff === null) {
            await Database.insertOne('sder.rawJurinet', decision);
            await Indexing.indexDecision('cc', decision, null, 'import in rawJurinet (sync)');
          } else {
            if (decisions[i].reprocess === true) {
              decision.IND_ANO = 0;
              decision.XMLA = null;
              if (decisions[i].anomaly === true) {
                await Indexing.updateDecision(
                  'cc',
                  decision,
                  null,
                  `update in rawJurinet and reprocessed (sync) - original text could have been changed - changelog: ${JSON.stringify(
                    decisions[i].diff,
                  )}`,
                );
              } else {
                await Indexing.updateDecision(
                  'cc',
                  decision,
                  null,
                  `update in rawJurinet and reprocessed (sync) - changelog: ${JSON.stringify(decisions[i].diff)}`,
                );
              }
            } else if (decisions[i].anomaly === true) {
              await Indexing.updateDecision(
                'cc',
                decision,
                null,
                `update in rawJurinet (sync) - original text could have been changed - changelog: ${JSON.stringify(
                  decisions[i].diff,
                )}`,
              );
            } else {
              await Indexing.updateDecision(
                'cc',
                decision,
                null,
                `update in rawJurinet (sync) - changelog: ${JSON.stringify(decisions[i].diff)}`,
              );
            }
            await Database.replaceOne('sder.rawJurinet', { _id: decision._id }, decision);
          }
          await Indexing.indexAffaire('cc', decision);

          let normalized = await Database.findOne('sder.decisions', { sourceId: decision._id, sourceName: 'jurinet' });
          if (normalized === null) {
            let normDec = await Indexing.normalizeDecision('ca', decision, null, false, true);
            const insertResult = await Database.insertOne('sder.decisions', normDec);
            normDec._id = insertResult.insertedId;
            await Indexing.indexDecision('sder', normDec, null, 'import in decisions (sync)');
          } else if (normalized.locked === false && decisions[i].diff !== null) {
            let normDec = await Indexing.normalizeDecision('ca', decision, normalized, false, true);
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
          const ShouldBeRejected = await Indexing.shouldBeRejected(
            'ca',
            decision.JDEC_CODNAC,
            decision.JDEC_CODNACPART,
            decision.JDEC_IND_DEC_PUB,
          );
          if (ShouldBeRejected === false) {
            let partiallyPublic = false;
            try {
              partiallyPublic = Indexing.isPartiallyPublic(
                'ca',
                decision.JDEC_CODNAC,
                decision.JDEC_CODNACPART,
                decision.JDEC_IND_DEC_PUB,
              );
            } catch (ignore) {}
            if (partiallyPublic) {
              // @TODO
            }
            await Database.insertOne('sder.rawJurica', decision);
            await Indexing.indexDecision('ca', decision, null, 'import in rawJurica');
            await Indexing.indexAffaire('ca', decision);
            const ShouldBeSentToJudifiltre = Indexing.shouldBeSentToJudifiltre(
              'ca',
              decision.JDEC_CODNAC,
              decision.JDEC_CODNACPART,
              decision.JDEC_IND_DEC_PUB,
            );
            if (ShouldBeSentToJudifiltre === true) {
              // @TODO
            } else {
              let normalized = await Database.findOne('sder.decisions', {
                sourceId: decision._id,
                sourceName: 'jurica',
              });
              if (normalized === null) {
                let normDec = await Indexing.normalizeDecision('ca', decision, null, false, true);
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
      { labelStatus: 'done', sourceName: 'jurinet' },
      { allowDiskUse: true },
    );

    return decisions;
  }

  async reinjectUsingDB(decisions) {
    for (let i = 0; i < decisions.length; i++) {
      const decision = decisions[i];
      try {
        // 1. Get the original decision from Jurinet:
        const sourceDecision = await Database.findOne(
          'si.jurinet',
          `SELECT *
          FROM DOCUMENT
          WHERE DOCUMENT.ID_DOCUMENT = :id`,
          [decision.sourceId],
        );
        if (sourceDecision && sourceDecision.XML) {
          // 2. Get the content of the original XML field to create the new XMLA field:
          let xmla = sourceDecision.XML;
          if (xmla.indexOf('<TEXTE_ARRET>') !== -1) {
            // 3. Reinject the <TEXTE_ARRET> tag but with the reencoded pseudonymized content,
            let pseudoText = decision.pseudoText.replace(/&/g, '&amp;').replace(/&amp;amp;/g, '&amp;');
            pseudoText = pseudoText.replace(/</g, '&lt;');
            pseudoText = pseudoText.replace(/>/g, '&gt;');
            pseudoText = pseudoText.replace(/"/g, '&quot;');
            pseudoText = pseudoText.replace(/'/g, '&apos;');
            xmla = xmla.replace(
              /<TEXTE_ARRET>[\s\S]*<\/TEXTE_ARRET>/gim,
              '<TEXTE_ARRET>' + pseudoText + '</TEXTE_ARRET>',
            );
            xmla = Database.encodeOracleText(xmla);
            // 4. Set the date:
            const now = new Date();
            // 5. Update query (which, contrary to the doc, requires xmla to be passed as a String):
            await Database.writeQuery(
              'si.jurinet',
              `UPDATE DOCUMENT
              SET XMLA=:xmla,
              IND_ANO=:ok,
              AUT_ANO=:label,
              DT_ANO=:datea,
              DT_MODIF=:dateb,
              DT_MODIF_ANO=:datec,
              DT_ENVOI_DILA=NULL
              WHERE ID_DOCUMENT=:id`,
              [xmla.toString('binary'), 2, 'LABEL', now, now, now, decision.sourceId],
            );
          } else {
            throw new Error(
              'reinjectUsingDB: <TEXTE_ARRET> tag not found: the document could be malformed or corrupted.',
            );
          }
        } else {
          throw new Error(`reinjectUsingDB: decision '${decision.sourceId}' not found or has no XML content.`);
        }
        const reinjected = await Database.findOne(
          'si.jurinet',
          `SELECT *
          FROM DOCUMENT
          WHERE DOCUMENT.ID_DOCUMENT = :id`,
          [decision.sourceId],
        );
        reinjected._indexed = null;
        reinjected.DT_ANO = new Date();
        reinjected.DT_MODIF = new Date();
        reinjected.DT_MODIF_ANO = new Date();
        await Database.replaceOne('sder.rawJurinet', { _id: reinjected._id }, reinjected);
        decision.labelStatus = 'exported';
        decision.dateCreation = new Date().toISOString();
        await Database.replaceOne('sder.decisions', { _id: decision._id }, decision);
        await Indexing.updateDecision('sder', decision, null, `reinject`);
      } catch (e) {
        logger.error(`Jurinet reinjection error processing decision ${decision._id}`, e);
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
