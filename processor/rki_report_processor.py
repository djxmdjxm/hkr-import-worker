# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

import xml.etree.ElementTree as ET
import xmlschema

import os

from common.logging import getLogger
from krebs_db import (
    Session,
    PatientReport,
    TumorReport,
    TNM,
    TumorHistology,
    TumorReportBreast,
    TumorReportColorectal,
    TumorReportProstate,
    TumorReportMelanoma,
    TumorSurgery,
    TumorRadiotherapy,
    RadiotherapySession,
    RadiotherapySessionPercutaneous,
    RadiotherapySessionBrachytherapy,
    RadiotherapySessionMetabolic,
    TumorSystemicTherapy,
    TumorFollowUp
)

# --- F9: Strukturierte XSD-Fehlermeldungen ---
# Eigene Exception-Klasse damit main.py XSD-Fehler von anderen Fehlern unterscheiden kann.
# info_dict enthaelt: error_type, category, hint, technical_message, path
class XsdValidationError(Exception):
    def __init__(self, info_dict: dict):
        self.info_dict = info_dict
        super().__init__(info_dict.get("technical_message", "XSD validation error"))


def _categorize_xsd_error(reason: str, path: str) -> dict:
    # Kategorisiert einen XSD-Validierungsfehler regelbasiert (kein KI).
    # Gibt ein dict mit category und hint zurueck.
    r = reason.lower()
    p = path.lower()
    # Reihenfolge beachten: spezifischere Checks zuerst
    # F11: Schema_Version-Mismatch schlägt als Enumeration-Fehler an (reason enthält "enumeration"),
    # wird aber fälschlich als invalid_code_value klassifiziert. Path-Check hat Vorrang.
    if "schema_version" in p:
        cat  = "wrong_schema_version"
        hint = ("Die Schema-Version in der XML-Datei wird nicht erkannt. "
                "Unterstuetzt werden: 3.0.4_RKI und 3.0.0.8a_RKI.")
    elif "not expected" in r or "not allowed" in r or "unexpected" in r:
        cat  = "wrong_schema_version"
        hint = ("Unbekannte Felder in der XML gefunden. Bitte pruefen Sie, ob die Datei "
                "dem oBDS 3.0.4_RKI oder 3.0.0.8a_RKI Standard entspricht.")
    elif "not facet-valid" in r or "enumeration" in r or "value must be one of" in r:
        cat  = "invalid_code_value"
        hint = "Ein Codierwert in der Datei ist ungueltig. Bitte pruefen Sie die betroffene Stelle in der Quelldatei."
    elif "not complete" in r or "missing" in r:
        cat  = "missing_required_field"
        hint = "Ein Pflichtfeld fehlt in der Datei. Bitte pruefen Sie die Vollstaendigkeit des Meldebogens."
    elif "pattern-valid" in r or "pattern" in r:
        cat  = "wrong_format"
        hint = "Ein Feld hat das falsche Format (z. B. Datum). Erwartet wird meist JJJJ-MM-TT."
    elif "namespace" in r:
        cat  = "wrong_namespace"
        hint = "Die Datei scheint kein gueltiger oBDS-Meldebogen zu sein. Bitte pruefen Sie die Dateiherkunft."
    else:
        cat  = "unknown"
        hint = "Die Datei enthaelt einen unbekannten Fehler. Bitte wenden Sie sich an Ihre IT-Stelle."
    return {
        "error_type":        "xsd_validation",
        "category":          cat,
        "technical_message": reason,
        "path":              path,
        "hint":              hint,
    }


# XSD schema used to validate incoming XML files before import.
# Version 3.0.4_RKI was provided by the HKR (Hamburgisches Krebsregister) in April 2024,
# replacing the previous 3.0.0.8a_RKI schema.
# Key changes in 3.0.4_RKI vs 3.0.0.8a_RKI:
#   - New fields: Sentinel_LK_untersucht, Sentinel_LK_befallen (sentinel lymph node data)
#   - Renamed: Anzahl_Tage_ST_Dauer -> Anzahl_Tage_Bestrahlung_Dauer
#   - Renamed: Anzahl_Tage_Diagnose_ST -> Anzahl_Tage_Diagnose_Bestrahlung
#   - ICD version list replaced by regex pattern (future-proof for new yearly editions)
# Map: ReportType -> XSD-Datei
# Neue Schema-Version hinzufuegen = XSD nach schemas/ kopieren + Eintrag hier erganzen
XSD_MAP = {
    'XML:oBDS_3.0.0.8a_RKI': 'schemas/oBDS_v3.0.0.8a_RKI_Schema.xsd',
    'XML:oBDS_3.0.4_RKI':    'schemas/oBDS_v3.0.4_RKI_Schema.xsd',
}


def execute(uid: str, file_path: str, report_type: str = 'XML:oBDS_3.0.4_RKI'):
    # file_path: path to the XML file on the shared Docker volume /data/uploads/
    logger = getLogger(f'rki_report_processor.{uid}')
    logger.info('executing report import...')

    # Open the XML file directly from disk — no base64 decoding needed
    with open(file_path, 'rb') as f:
        xml_file = f.read()
    logger.info(f'loaded file from {file_path}')

    xsd_path = XSD_MAP.get(report_type, XSD_MAP['XML:oBDS_3.0.4_RKI'])
    schema = xmlschema.XMLSchema(xsd_path)
    logger.info(f'loaded schema:{xsd_path} for report_type:{report_type}')
    # iter_errors() liefert alle XMLSchemaValidationError-Objekte.
    # Wir nehmen nur den ersten Fehler - aussagekraeftig genug fuer den Use Case.
    xsd_errors = list(schema.iter_errors(xml_file))
    if xsd_errors:
        first = xsd_errors[0]
        info_dict = _categorize_xsd_error(
            reason=first.reason or str(first),
            path=str(first.path) if first.path else "",
        )
        raise XsdValidationError(info_dict)

    logger.info(f'schema verified successfully')
    
    xml_dict = schema.to_dict(xml_file)
    # Invariante: Nach bestandener XSD-Validierung sind required Felder garantiert vorhanden.
    # .get() für alle XML-Element-Keys ist bewusste Konvention — der Processor muss keine
    # XSD-Optionalität kennen. Ändert sich ein Feld von required→optional, ist kein Code nötig.

    ## Remap XML to KREBS Database
    session = Session()

    register = xml_dict['Lieferregister']['@Register_ID']
    report_at = xml_dict['Lieferdatum']['$']

    patient_reports_raw = xml_dict.get('Menge_Patient', {}).get('Patient', [])
    for patient_report_raw in patient_reports_raw:
        death_causes = list(map(
            lambda x: { 'code': x['Code'], 'version': x.get('Version') },
            patient_report_raw['Patienten_Stammdaten']['Vitalstatus']
                .get('Todesursachen', {})
                .get('Menge_Weitere_Todesursachen', {})
                .get('Todesursache_ICD', [])
        )) 
        
        underlying_death_cause = patient_report_raw['Patienten_Stammdaten']['Vitalstatus'].get('Todesursachen', {}).get('Grundleiden')
        if underlying_death_cause is not None:
            death_causes += [{
                'code': underlying_death_cause['Code'],
                'version': underlying_death_cause.get('Version'),
                'is_underlying': True
            }]

        patient_report = PatientReport(
            patient_id                 = patient_report_raw['@Patient_ID'],
            gender                     = patient_report_raw['Patienten_Stammdaten']['Geschlecht'],
            date_of_birth              = patient_report_raw['Patienten_Stammdaten']['Geburtsdatum']['$'],
            date_of_birth_accuracy     = patient_report_raw['Patienten_Stammdaten']['Geburtsdatum']['@Datumsgenauigkeit'],
            is_deceased                = patient_report_raw['Patienten_Stammdaten']['Vitalstatus']['Verstorben'] == 'J',
            vital_status_date          = patient_report_raw['Patienten_Stammdaten'].get('Vitalstatus', {}).get('Datum_Vitalstatus', {}).get('$'),
            vital_status_date_accuracy = patient_report_raw['Patienten_Stammdaten'].get('Vitalstatus', {}).get('Datum_Vitalstatus', {}).get('@Datumsgenauigkeit'),
            death_causes               = death_causes,
            register                   = register,
            reported_at                = report_at
        )

        patient_report.tumor_reports = []
        tumor_reports_raw = patient_report_raw.get('Menge_Tumor', {}).get('Tumor', [])
        for tumor_report_raw in tumor_reports_raw:
            tumor_icd = tumor_report_raw['Primaerdiagnose'].get('Primaertumor_ICD')
            tumor_topo_icd = tumor_report_raw['Primaerdiagnose'].get('Primaertumor_Topographie_ICD_O')
            distant_metastasis = list(map(
                lambda x: { 'location': x['Lokalisation'] },
                tumor_report_raw['Primaerdiagnose'].get('Menge_FM', {}).get('Fernmetastase', [])
            ))
            other_classification = list(map(
                lambda x: { 'name': x['Name'], 'stadium': x['Stadium'] },
                tumor_report_raw['Primaerdiagnose'].get('Menge_Weitere_Klassifikation', {}).get('Weitere_Klassifikation', [])
            ))
            tumor_report = TumorReport(
                tumor_id                = tumor_report_raw['@Tumor_ID'],
                diagnosis_date          = tumor_report_raw['Primaerdiagnose']['Diagnosedatum']['$'],
                diagnosis_date_accuracy = tumor_report_raw['Primaerdiagnose']['Diagnosedatum']['@Datumsgenauigkeit'],
                incidence_location      = tumor_report_raw['Primaerdiagnose']['Inzidenzort'],
                icd                     = {'code': tumor_icd['Code'], 'version': tumor_icd.get('Version')},
                topographie             = {'code': tumor_topo_icd['Code'], 'version': tumor_topo_icd.get('Version')} if tumor_topo_icd is not None else None,
                diagnostic_certainty    = tumor_report_raw['Primaerdiagnose']['Diagnosesicherung'],
                distant_metastasis      = distant_metastasis if len(distant_metastasis) > 0 else None,
                other_classification    = other_classification if len(other_classification) > 0 else None,
                laterality              = tumor_report_raw['Primaerdiagnose']['Seitenlokalisation']
            )

            histology_raw = tumor_report_raw['Primaerdiagnose'].get('Histologie')
            if histology_raw is not None:
                icd = histology_raw.get('Morphologie_ICD_O')
                histology = TumorHistology(
                    morphology_icd       = { 'code': icd['Code'], 'version': icd.get('Version') },
                    grading              = histology_raw.get('Grading'),
                    lymph_nodes_examined = histology_raw.get('LK_untersucht'),
                    lymph_nodes_affected = histology_raw.get('LK_befallen')
                )
                tumor_report.histology = histology

            breast_raw = tumor_report_raw['Primaerdiagnose'].get('Modul_Mamma')
            if breast_raw is not None:
                breast = TumorReportBreast(
                    menopause_status_at_diagnosis = breast_raw.get('Praetherapeutischer_Menopausenstatus'),
                    estrogen_receptor_status      = breast_raw.get('HormonrezeptorStatus_Oestrogen'),
                    progesterone_receptor_status  = breast_raw.get('HormonrezeptorStatus_Progesteron'),
                    her2neu_status                = breast_raw.get('Her2neuStatus'),
                    tumor_size_mm_invasive        = breast_raw.get('TumorgroesseInvasiv'),
                    tumor_size_mm_dcis            = breast_raw.get('TumorgroesseDCIS')
                )
                tumor_report.breast = breast

            colorectal_raw = tumor_report_raw['Primaerdiagnose'].get('Modul_Darm')
            if colorectal_raw is not None:
                colorectal = TumorReportColorectal(
                    ras_mutation                         = colorectal_raw.get('RASMutation'),
                    rectum_distance_anocutaneous_line_cm = colorectal_raw.get('RektumAbstandAnokutanlinie')
                )
                tumor_report.colorectal = colorectal

            prostate_raw = tumor_report_raw['Primaerdiagnose'].get('Modul_Prostata')
            if prostate_raw is not None:
                prostate = TumorReportProstate(
                    gleason_primary_grade   = prostate_raw.get('GleasonScore', {}).get('GradPrimaer'),
                    gleason_secondary_grade = prostate_raw.get('GleasonScore', {}).get('GradSekundaer'),
                    gleason_score_result    = prostate_raw.get('GleasonScore', {}).get('ScoreErgebnis'),
                    gleason_score_reason    = prostate_raw.get('AnlassGleasonScore'),
                    psa                     = prostate_raw.get('PSA'),
                    psa_date                = prostate_raw.get('DatumPSA', {}).get('$'),
                    psa_date_accuracy       = prostate_raw.get('DatumPSA', {}).get('Datumsgenauigkeit')
                )
                tumor_report.prostate = prostate
            
            melanoma_raw = tumor_report_raw['Primaerdiagnose'].get('Modul_Malignes_Melanom')
            if melanoma_raw is not None:
                melanoma = TumorReportMelanoma(
                    tumor_thickness_mm = melanoma_raw.get('Tumordicke'),
                    ldh                = melanoma_raw.get('LDH'),
                    ulceration         = melanoma_raw.get('Ulzeration').text == 'J'
                )
                tumor_report.melanoma = melanoma

            # cTNM = clinical TNM staging (before therapy, based on imaging/examination).
            # pTNM = pathological TNM staging (after surgery, based on tissue specimen).
            # Both are optional in the schema — a tumor may have one, both, or neither.
            # BUG FIX (HKR 2024): the original code crashed when pTNM existed without cTNM,
            # because it assumed cTNM was always present. Both blocks are now fully independent
            # and each guarded by 'if ... is not None' before accessing any fields.
            cTNM_raw = tumor_report_raw['Primaerdiagnose'].get('cTNM')
            if cTNM_raw is not None:
                cTNM = TNM(
                    version    = cTNM_raw.get('Version'),
                    y_symbol   = cTNM_raw.get('y_Symbol') == 'y',
                    r_symbol   = cTNM_raw.get('r_Symbol') == 'r',
                    a_symbol   = cTNM_raw.get('a_Symbol') == 'a',
                    t_prefix   = cTNM_raw.get('c_p_u_Praefix_T'),
                    t          = cTNM_raw.get('T'),
                    m_symbol   = cTNM_raw.get('m_Symbol'),
                    n_prefix   = cTNM_raw.get('c_p_u_Praefix_N'),
                    n          = cTNM_raw.get('N'),
                    m_prefix   = cTNM_raw.get('c_p_u_Praefix_M'),
                    m          = cTNM_raw.get('M'),
                    l          = cTNM_raw.get('L'),
                    v          = cTNM_raw.get('V'),
                    pn         = cTNM_raw.get('Pn'),
                    s          = cTNM_raw.get('S'),
                    uicc_stage = cTNM_raw.get('UICC_Stadium')
                )
                tumor_report.c_tnm = cTNM

            pTNM_raw = tumor_report_raw['Primaerdiagnose'].get('pTNM')
            if pTNM_raw is not None:
                pTNM = TNM(
                    version    = pTNM_raw.get('Version'),
                    y_symbol   = pTNM_raw.get('y_Symbol') == 'y',
                    r_symbol   = pTNM_raw.get('r_Symbol') == 'r',
                    a_symbol   = pTNM_raw.get('a_Symbol') == 'a',
                    t_prefix   = pTNM_raw.get('c_p_u_Praefix_T'),
                    t          = pTNM_raw.get('T'),
                    m_symbol   = pTNM_raw.get('m_Symbol'),
                    n_prefix   = pTNM_raw.get('c_p_u_Praefix_N'),
                    n          = pTNM_raw.get('N'),
                    m_prefix   = pTNM_raw.get('c_p_u_Praefix_M'),
                    m          = pTNM_raw.get('M'),
                    l          = pTNM_raw.get('L'),
                    v          = pTNM_raw.get('V'),
                    pn         = pTNM_raw.get('Pn'),
                    s          = pTNM_raw.get('S'),
                    uicc_stage = pTNM_raw.get('UICC_Stadium')
                )
                tumor_report.p_tnm = pTNM

            tumor_report.surgeries = []
            surgeries_raw = tumor_report_raw.get('Menge_OP', {}).get('OP', [])
            for surgery_raw in surgeries_raw:
                operations = list(map(
                    lambda x: { 'code': x['Code'], 'version': x['Version'] },
                    surgery_raw.get('Menge_OPS', {}).get('OPS', [])
                ))
                surgery = TumorSurgery(
                    intent                = surgery_raw.get('Intention'),
                    date                  = surgery_raw.get('Datum_OP', {})['$'],
                    date_accuracy         = surgery_raw.get('Datum_OP', {})['@Datumsgenauigkeit'],
                    operations            = operations,
                    local_residual_status = surgery_raw.get('Lokale_Beurteilung_Residualstatus')
                )
                tumor_report.surgeries.append(surgery)

            tumor_report.radiotherapies = []
            radiotherapies_raw = tumor_report_raw.get('Menge_ST', {}).get('ST', [])
            for radiotherapy_raw in radiotherapies_raw:
                radiotherapy = TumorRadiotherapy(
                    intent           = radiotherapy_raw.get('Intention'),
                    surgery_relation = radiotherapy_raw.get('Stellung_OP')
                )

                radiotherapy.sessions = []
                radiotherapy_sessions_raw = radiotherapy_raw.get('Menge_Bestrahlung', {}).get('Bestrahlung', [])
                for radiotherapy_session_raw in radiotherapy_sessions_raw:
                    application_type = radiotherapy_session_raw.get('Applikationsart', {})
                    target_area = application_type.get('Perkutan',
                        application_type.get('Kontakt',
                            application_type.get('Metabolisch',
                                application_type.get('Sonstige',
                                    application_type.get('Unbekannt', {})
                                )
                            )
                        ),
                    ).get('Zielgebiet', {})
                    laterality = application_type.get('Perkutan',
                        application_type.get('Kontakt',
                            application_type.get('Metabolisch',
                                application_type.get('Sonstige',
                                    application_type.get('Unbekannt', {})
                                )
                            )
                        ),
                    ).get('Seite_Zielgebiet', {})
                    
                    radiotherapy_session = RadiotherapySession(
                        start_date          = radiotherapy_session_raw.get('Datum_Beginn_Bestrahlung', {})['$'],
                        start_date_accuracy = radiotherapy_session_raw.get('Datum_Beginn_Bestrahlung', {})['@Datumsgenauigkeit'],
                        duration_days       = radiotherapy_session_raw.get('Anzahl_Tage_ST_Dauer'),
                        target_area         = target_area.get('CodeVersion2021', target_area.get('CodeVersion2014')),
                        laterality          = laterality,
                    )

                    percutaneous_raw = radiotherapy_session_raw.get('Applikationsart').get('Perkutan')
                    if percutaneous_raw is not None:
                        percutaneous = RadiotherapySessionPercutaneous(
                            chemoradio        = percutaneous_raw.get('Radiochemo'),
                            stereotactic      = percutaneous_raw.get('Stereotaktisch') is not None,
                            respiratory_gated = percutaneous_raw.get('Atemgetriggert') is not None
                        )
                        radiotherapy_session.percutaneous = percutaneous

                    brachytherapy_raw = radiotherapy_session_raw.get('Applikationsart').get('Kontakt')
                    if brachytherapy_raw is not None:
                        brachytherapy = RadiotherapySessionBrachytherapy(
                            type      = brachytherapy_raw.get('Interstitiell_endokavitaer'),
                            dose_rate = brachytherapy_raw.get('Rate_Type')
                        )
                        radiotherapy_session.brachytherapy = brachytherapy
                    
                    metabolic_raw = radiotherapy_session_raw.get('Applikationsart').get('Metabolisch')
                    if metabolic_raw is not None:
                        metabolic = RadiotherapySessionMetabolic(
                            type = metabolic_raw.get('Metabolisch_Typ')
                        )
                        radiotherapy_session.metabolic = metabolic
                    
                    radiotherapy.sessions.append(radiotherapy_session)

                tumor_report.radiotherapies.append(radiotherapy)

            tumor_report.systemic_therapies = []
            systemic_therapies_raw = tumor_report_raw.get('Menge_SYST', {}).get('SYST', [])
            for systemic_therapy_raw in systemic_therapies_raw:
                protocol = systemic_therapy_raw.get('Protokoll', {})
                protocol_name = protocol.get('Bezeichnung')
                protocol_obj = protocol.get('Protokollschluessel')
                drugs = list(map(
                    lambda x: { 'name': x['Bezeichnung'] } if x.get('Bezeichnung') is not None else
                            { 'code': x['ATC']['Code'], 'version': x['ATC'].get('Version') } if x.get('ATC') is not None else
                            None,
                    systemic_therapy_raw.get('Menge_Substanz', {}).get('Substanz', [])
                ))

                systemic_therapy = TumorSystemicTherapy(
                    start_date          = systemic_therapy_raw.get('Datum_Beginn_SYST', {})['$'],
                    start_date_accuracy = systemic_therapy_raw.get('Datum_Beginn_SYST', {})['@Datumsgenauigkeit'],
                    duration_days       = systemic_therapy_raw.get('Anzahl_Tage_SYST_Dauer'),
                    intent              = systemic_therapy_raw.get('Intention'),
                    surgery_relation    = systemic_therapy_raw.get('Stellung_OP'),
                    type                = systemic_therapy_raw.get('Therapieart'),
                    protocol            = { 'name': protocol_name } if protocol_name is not None else 
                                        { 'code': protocol_obj.get('Code'), 'version': protocol_obj.get('Version') } if protocol_obj is not None else
                                        None,
                    drugs               = drugs
                )
                tumor_report.systemic_therapies.append(systemic_therapy)
            
            tumor_report.follow_ups = []
            follow_ups_raw = tumor_report_raw.get('Menge_Folgeereignis', {}).get('Folgeereignis', [])
            for follow_up_raw in follow_ups_raw:
                other_classification = list(map(
                    lambda x: { 'name': x['Name'], 'stadium': x['Stadium'] },
                    follow_up_raw.get('Menge_Weitere_Klassifikation', {}).get('Weitere_Klassifikation', [])
                ))
                distant_metastasis = list(map(
                    lambda x: { 'location': x['Lokalisation'] },
                    follow_up_raw.get('Menge_FM', {}).get('Fernmetastase', [])
                ))
                follow_up = TumorFollowUp(
                    other_classification            = other_classification,
                    date                            = follow_up_raw.get('Datum_Folgeereignis', {})['$'],
                    date_accuracy                   = follow_up_raw.get('Datum_Folgeereignis', {})['@Datumsgenauigkeit'],
                    overall_tumor_status            = follow_up_raw.get('Gesamtbeurteilung_Tumorstatus'),
                    local_tumor_status              = follow_up_raw.get('Verlauf_Lokaler_Tumorstatus'),
                    lymph_node_tumor_status         = follow_up_raw.get('Verlauf_Tumorstatus_Lymphknoten'),
                    distant_metastasis_tumor_status = follow_up_raw.get('Verlauf_Tumorstatus_Fernmetastasen'),
                    distant_metastasis              = distant_metastasis,
                )

                tnm_raw = follow_up_raw.get('TNM')
                if tnm_raw is not None:
                    tnm = TNM(
                        version    = tnm_raw.get('Version'),
                        y_symbol   = tnm_raw.get('y_Symbol') == 'y',
                        r_symbol   = tnm_raw.get('r_Symbol') == 'r',
                        a_symbol   = tnm_raw.get('a_Symbol') == 'a',
                        t_prefix   = tnm_raw.get('c_p_u_Praefix_T'),
                        t          = tnm_raw.get('T'),
                        m_symbol   = tnm_raw.get('m_Symbol'),
                        n_prefix   = tnm_raw.get('c_p_u_Praefix_N'),
                        n          = tnm_raw.get('N'),
                        m_prefix   = tnm_raw.get('c_p_u_Praefix_M'),
                        m          = tnm_raw.get('M'),
                        l          = tnm_raw.get('L'),
                        v          = tnm_raw.get('V'),
                        pn         = tnm_raw.get('Pn'),
                        s          = tnm_raw.get('S'),
                        uicc_stage = tnm_raw.get('UICC_Stadium')
                    )
                    follow_up.tnm = tnm
                
                tumor_report.follow_ups.append(follow_up)

            patient_report.tumor_reports.append(tumor_report)

        session.add(patient_report)
        session.commit()

        logger.info(f'imported patient with id:{patient_report.patient_id}')

    session.close()

    # Delete the XML file from the shared volume after successful import
    try:
        os.remove(file_path)
        logger.info(f'deleted upload file {file_path}')
    except OSError as e:
        logger.warning(f'could not delete upload file {file_path}: {e}')

