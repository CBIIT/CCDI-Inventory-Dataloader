 MATCH (sm:sample)
        OPTIONAL MATCH (p:participant)<-[*..3]-(sm)
        optional match (sm)<-[*..3]-(file)
        WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        MATCH (st:study)<-[:of_participant]-(p)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
        OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
        OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
        WITH file, su, p, st, sm, stf, stp, dg
        RETURN DISTINCT
          sm.id as id,
          p.id as pid,
          sm.sample_id as sample_id,
          p.participant_id as participant_id,
          apoc.text.split(p.race, ';') as race,
          p.sex_at_birth as sex_at_birth,
          sm.anatomic_site as sample_anatomic_site,
          sm.diagnosis_classification as sample_diagnosis_classification,
          sm.diagnosis_classification_system as sample_diagnosis_classification_system,
          sm.diagnosis_verification_status as sample_diagnosis_verification_status,
          sm.diagnosis_basis as sample_diagnosis_basis,
          sm.diagnosis_comment as sample_diagnosis_comment,
          sm.participant_age_at_collection as participant_age_at_collection,
          sm.sample_tumor_status as sample_tumor_status,
          sm.tumor_classification as tumor_classification,
          st.study_id as study_id,
          st.dbgap_accession as dbgap_accession,
          st.study_acronym as study_acronym,
          st.study_short_title as study_short_title,
          COLLECT(DISTINCT {
              age_at_diagnosis: dg.age_at_diagnosis,
              diagnosis_anatomic_site: dg.anatomic_site,
              disease_phase: dg.disease_phase,
              diagnosis_classification_system: dg.diagnosis_classification_system,
              diagnosis_verification_status: dg.diagnosis_verification_status,
              diagnosis_basis: dg.diagnosis_basis,
              diagnosis_comment: dg.diagnosis_comment,
              tumor_grade_source: dg.tumor_grade_source,
              tumor_stage_source: dg.tumor_stage_source,
              diagnosis: dg.diagnosis
          }) AS diagnosis_filters,
          su.last_known_survival_status as last_known_survival_status,
          COLLECT(DISTINCT {
              assay_method: CASE LABELS(file)[0]
                        WHEN 'sequencing_file' THEN 'Sequencing'
                        WHEN 'single_cell_sequencing_file' THEN 'Single Cell Sequencing'
                        WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                        WHEN 'pathology_file' THEN 'Pathology imaging'
                        WHEN 'methylation_array_file' THEN 'Methylation array' 
                        ELSE null END,
              file_type: file.file_type,
              library_selection: CASE LABELS(file)[0]
                            WHEN 'sequencing_file' THEN file.library_selection
                            WHEN 'single_cell_sequencing_file' THEN file.library_selection
                            ELSE null END,
              library_source: CASE LABELS(file)[0]
                            WHEN 'sequencing_file' THEN file.library_source
                            WHEN 'single_cell_sequencing_file' THEN file.library_source
                            ELSE null END,
              library_strategy: CASE LABELS(file)[0]
                            WHEN 'sequencing_file' THEN file.library_strategy
                             WHEN 'single_cell_sequencing_file' THEN file.library_strategy
                            ELSE null END
          }) AS file_filters,
          COLLECT(DISTINCT stf.grant_id) as grant_id,
          COLLECT(DISTINCT stp.institution) as institution,
          COUNT(DISTINCT file.id) as file_count,
          COLLECT(DISTINCT file.id) as files
        union all
        MATCH (sm:sample)
        MATCH (st:study)<-[:of_cell_line|of_pdx]-(cl)<--(sm)
        Where (cl:cell_line or cl:pdx)
        optional Match (sm)<--(file)
        WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        MATCH (st)<-[:of_participant]-(p:participant)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
        OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
        OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
        WITH sm, file, su, st, stf, stp, dg
        RETURN DISTINCT
          sm.id as id,
          null as pid,
          sm.sample_id as sample_id,
          null as participant_id,
          null as race,
          null as sex_at_birth,
          sm.anatomic_site as sample_anatomic_site,
          sm.diagnosis_classification as sample_diagnosis_classification,
          sm.diagnosis_classification_system as sample_diagnosis_classification_system,
          sm.diagnosis_verification_status as sample_diagnosis_verification_status,
          sm.diagnosis_basis as sample_diagnosis_basis,
          sm.diagnosis_comment as sample_diagnosis_comment,
          sm.participant_age_at_collection as participant_age_at_collection,
          sm.sample_tumor_status as sample_tumor_status,
          sm.tumor_classification as tumor_classification,
          st.study_id as study_id,
          st.dbgap_accession as dbgap_accession,
          st.study_acronym as study_acronym,
          st.study_short_title as study_short_title,
          COLLECT(DISTINCT {
              age_at_diagnosis: dg.age_at_diagnosis,
              diagnosis_anatomic_site: dg.anatomic_site,
              disease_phase: dg.disease_phase,
              diagnosis_classification_system: dg.diagnosis_classification_system,
              diagnosis_verification_status: dg.diagnosis_verification_status,
              diagnosis_basis: dg.diagnosis_basis,
              diagnosis_comment: dg.diagnosis_comment,
              tumor_grade_source: dg.tumor_grade_source,
              tumor_stage_source: dg.tumor_stage_source,
              diagnosis: dg.diagnosis
          }) AS diagnosis_filters,
          su. last_known_survival_status as last_known_survival_status,
          CASE COLLECT(file) WHEN [] THEN []
                    ELSE COLLECT(DISTINCT {
                        assay_method: CASE LABELS(file)[0]
                                  WHEN 'sequencing_file' THEN 'Sequencing'
                                  WHEN 'single_cell_sequencing_file' THEN 'Single Cell Sequencing'
                                  WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                                  WHEN 'pathology_file' THEN 'Pathology imaging'
                                  WHEN 'methylation_array_file' THEN 'Methylation array' 
                                  ELSE null END,
                        file_type: file.file_type,
                        library_selection: CASE LABELS(file)[0]
                                      WHEN 'sequencing_file' THEN file.library_selection
                                      WHEN 'single_cell_sequencing_file' THEN file.library_selection
                                      ELSE null END,
                        library_source: CASE LABELS(file)[0]
                                      WHEN 'sequencing_file' THEN file.library_source
                                      WHEN 'single_cell_sequencing_file' THEN file.library_source
                                      ELSE null END,
                        library_strategy: CASE LABELS(file)[0]
                                      WHEN 'sequencing_file' THEN file.library_strategy
                                      WHEN 'single_cell_sequencing_file' THEN file.library_strategy
                                      ELSE null END
                    }) END AS file_filters,
          COLLECT(DISTINCT stf.grant_id) as grant_id,
          COLLECT(DISTINCT stp.institution) as institution,
          COUNT(DISTINCT file.id) as file_count,
          COLLECT(DISTINCT file.id) as files