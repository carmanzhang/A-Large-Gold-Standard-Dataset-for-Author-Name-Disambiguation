create database if not exists and_ds;

create materialized view if not exists and_ds.orcid_simple
            ENGINE = MergeTree partition by length(path) > 0 order by length(path) > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select item.1  as title,
       item.2  as pub_type,
       item.3  as journal_title,
       item.4  as pub_date,
       item.5  as external_id_arr,
       item.6  as source,
       item.7  as source_uri,
       item.8  as source_path,
       item.9  as source_host,
       item.10 as assert_source_uri,
       item.11 as assert_source_path,
       item.12 as assert_source_host,
       item.13 as last_modified_date,
       item.14 as created_date,
       item.15 as path,
       item.16 as visibility,
       item.17 as display_index,
       first_name,
       last_name,
       credit_name,
       other_names,
       country,
       biography,
       emails,
       external_ids,
       keywords,
       orcid_lastmodifieddate,
       person_path,
       researcher_uris,
       orcid_id_host,
       orcid_id_uri,
       claimed,
       orcid_completion_date,
       creation_method,
       orcid_deactivation_date,
       history_modifieddate,
       orcid_submission_date,
       activity_lastmodifieddate,
       verified_email,
       verified_primary_email,
       orcid_path,
       locale,
       orcid_type
from (
         select JSONExtractString(JSONExtractRaw(name, 'givenNames'), 'content')                        as first_name,
                JSONExtractString(JSONExtractRaw(name, 'familyName'), 'content')                        as last_name,
                JSONExtractString(JSONExtractRaw(name, 'creditName'), 'content')                        as credit_name,
                arrayMap(x->JSONExtractString(x, 'content'),
                         JSONExtractArrayRaw(other_names, 'otherNames'))                                as other_names,
                arrayMap(x->JSONExtractString(JSONExtractRaw(x, 'country'), 'value'),
                         JSONExtractArrayRaw(addresses, 'address'))                                     as country,
                JSONExtractString(biography, 'content')                                                 as biography,
                arrayMap(x->JSONExtractString(x, 'email'), JSONExtractArrayRaw(emails, 'emails'))       as emails,
                arrayMap(z->
                             [
                                 JSONExtractString(z, 'type'),
                                 JSONExtractString(z, 'value'),
                                 JSONExtractString(JSONExtractRaw(z, 'url'), 'value'),
                                 JSONExtractString(z, 'relationship')
                                 ],
                         JSONExtractArrayRaw(external_ids, 'externalIdentifiers'))                      as external_ids,
                arrayMap(x->JSONExtractString(x, 'content'), JSONExtractArrayRaw(keywords, 'keywords')) as keywords,
                arrayStringConcat([
                                      toString(JSONExtractInt(
                                              JSONExtractRaw(orcid_last_modified_date, 'value') as orcid_last_modified_date_tmp,
                                              'year')),
                                      toString(JSONExtractInt(orcid_last_modified_date_tmp, 'month')),
                                      toString(JSONExtractInt(orcid_last_modified_date_tmp, 'day'))],
                                  '-')                                                                  as orcid_lastmodifieddate,
                person_path,
                arrayMap(x->
                             [JSONExtractString(x, 'urlName'),
                                 JSONExtractString(JSONExtractRaw(x, 'url'), 'value')
                                 ],
                         JSONExtractArrayRaw(researcher_urls, 'researcherUrls'))                        as researcher_uris,
                orcid_id_host,
                orcid_id_uri,
                claimed,
                arrayStringConcat([
                                      toString(JSONExtractInt(
                                              JSONExtractRaw(completion_date, 'value') as completion_date_tmp,
                                              'year')),
                                      toString(JSONExtractInt(completion_date_tmp, 'month')),
                                      toString(JSONExtractInt(completion_date_tmp, 'day'))],
                                  '-')                                                                  as orcid_completion_date,
                creation_method,
                arrayStringConcat([
                                      toString(JSONExtractInt(
                                              JSONExtractRaw(deactivation_date, 'value') as deactivation_date_tmp,
                                              'year')),
                                      toString(JSONExtractInt(deactivation_date_tmp, 'month')),
                                      toString(JSONExtractInt(deactivation_date_tmp, 'day'))],
                                  '-')                                                                  as orcid_deactivation_date,
                arrayStringConcat([
                                      toString(JSONExtractInt(
                                              JSONExtractRaw(history_last_modified_date, 'value') as history_tmp,
                                              'year')),
                                      toString(JSONExtractInt(history_tmp, 'month')),
                                      toString(JSONExtractInt(history_tmp
                                          , 'day'))],
                                  '-')                                                                  as history_modifieddate,
                arrayStringConcat([
                                      toString(JSONExtractInt(
                                              JSONExtractRaw(submission_date, 'value') as submission_date_tmp,
                                              'year')),
                                      toString(JSONExtractInt(submission_date_tmp, 'month')),
                                      toString(JSONExtractInt(submission_date_tmp
                                          , 'day'))],
                                  '-')                                                                  as orcid_submission_date,
                arrayStringConcat([
                                      toString(JSONExtractInt(
                                              JSONExtractRaw(activity_last_modified_date, 'value') as activity_tmp,
                                              'year')),
                                      toString(JSONExtractInt(activity_tmp, 'month')),
                                      toString(JSONExtractInt(activity_tmp
                                          , 'day'))],
                                  '-')                                                                  as activity_lastmodifieddate,
                verified_email,
                verified_primary_email,
                path                                                                                    as orcid_path,
                locale,
                orcid_type,
                arrayMap(x->
                             arrayMap(y->
                                          (
                                           JSONExtractString(JSONExtractRaw(JSONExtractRaw(y, 'title'), 'title'),
                                                             'content'),
                                           JSONExtractString(y, 'type'),
                                           JSONExtractString(JSONExtractRaw(y, 'journalTitle'), 'content'),
                                           arrayStringConcat([JSONExtractString(
                                                   JSONExtractRaw(JSONExtractRaw(y, 'publicationDate') as pub_date_str,
                                                                  'year'), 'value'),
                                                                 JSONExtractString(JSONExtractRaw(pub_date_str, 'month'), 'value'),
                                                                 JSONExtractString(JSONExtractRaw(pub_date_str, 'day'), 'value')],
                                                             '-') as pub_date,
                                           arrayMap(z->
                                                        [
                                                            JSONExtractString(z, 'type'),
                                                            JSONExtractString(z, 'value'),
                                                            JSONExtractString(JSONExtractRaw(z, 'url'), 'value'),
                                                            JSONExtractString(z, 'relationship')
                                                            ],
                                                    JSONExtractArrayRaw(JSONExtractRaw(y, 'externalIdentifiers'),
                                                                        'externalIdentifiers')),
                                           JSONExtractString(
                                                   JSONExtractRaw(JSONExtractRaw(y, 'source') as source, 'sourceName'),
                                                   'content'),
                                           JSONExtractString(JSONExtractRaw(source, 'sourceClientId'), 'uri'),
                                           JSONExtractString(JSONExtractRaw(source, 'sourceClientId'), 'path'),
                                           JSONExtractString(JSONExtractRaw(source, 'sourceClientId'), 'host'),
                                           JSONExtractString(JSONExtractRaw(source, 'assertionOriginOrcid'), 'uri'),
                                           JSONExtractString(JSONExtractRaw(source, 'assertionOriginOrcid'), 'path'),
                                           JSONExtractString(JSONExtractRaw(source, 'assertionOriginOrcid'), 'host'),
                                           arrayStringConcat([
                                                                 toString(JSONExtractInt(
                                                                         JSONExtractRaw(JSONExtractRaw(y, 'lastModifiedDate'), 'value') as last_modified_date,
                                                                         'year')),
                                                                 toString(JSONExtractInt(last_modified_date, 'month')),
                                                                 toString(JSONExtractInt(last_modified_date, 'day'))],
                                                             '-'),
                                           arrayStringConcat([
                                                                 toString(JSONExtractInt(
                                                                         JSONExtractRaw(JSONExtractRaw(y, 'createdDate'), 'value') as created_date,
                                                                         'year')),
                                                                 toString(JSONExtractInt(created_date, 'month')),
                                                                 toString(JSONExtractInt(created_date, 'day'))], '-'),
                                           JSONExtractString(y, 'path'),
                                           JSONExtractString(y, 'visibility'),
                                           JSONExtractString(y, 'displayIndex')
                                              ),
                                      JSONExtractArrayRaw(x, 'workSummary')),
                         JSONExtractArrayRaw(works, 'workGroup'))                                       as arr
         from orcid.orcid)
         array join arr as item
;

create materialized view if not exists and_ds.orcid_author_paper_doi
            ENGINE = MergeTree partition by length(orcid) > 0 order by length(orcid) > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select arrayDistinct(
               arrayFlatten(
                       arrayMap(x->arrayMap(
                               z->replaceAll(trimBoth(lowerUTF8(z[2])), 'https://doi.org/', ''),
                               arrayFilter(y->y[1] = 'doi', x)),
                                external_id_arr)))[1] as doi,
       substring(orcid_path, 2)                       as orcid,
       position(last_name, '.') == 0 or
       arrayCount(x->length(x) > 1, splitByChar(' ', replaceRegexpAll(replaceAll(last_name, '.', ' '), '\\s+', ' '))) >
       0                                              as do_not_need_substitution,
       do_not_need_substitution ?
       [credit_name,last_name,first_name] :
       [credit_name,first_name,last_name]             as orcid_names
from and_ds.orcid_simple
where length(doi) > 0;

create materialized view if not exists and_ds.mag_authors_half_0
            ENGINE = MergeTree partition by PaperId > 0 order by PaperId > 0
                settings storage_policy = 'moving_from_sda_to_sdb2'
            populate
as
select PaperId,
       arraySort(
               x->toInt16(x[1]),
               groupUniqArray(author_name_position)) as authors
from (select PaperId, [toString(AuthorSequenceNumber), lower(OriginalAuthor)] as author_name_position
      from mag.paper_author_affiliation
      where PaperId <= 2300000000)
group by PaperId;

create materialized view if not exists and_ds.mag_authors_half_1
            ENGINE = MergeTree partition by PaperId > 0 order by PaperId > 0
                settings storage_policy = 'moving_from_sda_to_sdb2'
            populate
as
select PaperId,
       arraySort(
               x->toInt16(x[1]),
               groupUniqArray(author_name_position)) as authors
from (select PaperId, [toString(AuthorSequenceNumber), lower(OriginalAuthor)] as author_name_position
      from mag.paper_author_affiliation
      where PaperId > 2300000000)
group by PaperId;

create view if not exists and_ds.mag_authors as
select *
from and_ds.mag_authors_half_0
union all
select *
from and_ds.mag_authors_half_1;

create materialized view if not exists and_ds.orcid_mag_matched_paper_author
            ENGINE = MergeTree partition by length(orcid) > 0 order by length(orcid) > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select pid,
       orcid,
       orcid_names,
       biblio_authors,
       matched_author_order1,
       matched_biblio_author
from (
      select pid,
             orcid,
             orcid_names,
             authors                                                               as biblio_authors,
             arrayMap(x->replaceAll(replaceAll(x, '.', ''), ' ', ''),
                      biblio_authors)                                              as biblio_clean_authors,
             replaceAll(replaceAll(lower(concat(orcid_names[-1], orcid_names[-2])), '.', ''), ' ',
                        '')                                                        as orcid_clean_name,
             arrayMap(biblio_name->
                              len > 0 ? 2 * length(
                                  arrayIntersect(arrayMap(y->substring(biblio_name, y + 1, 2),
                                                          range(toUInt16(length(biblio_name) - 1))) as gram2_arr,
                                                 arrayMap(y->substring(orcid_clean_name, y + 1, 2),
                                                          range(toUInt16(length(orcid_clean_name) - 1))) as gram2_arr1)) /
                                        (length(gram2_arr) + length(gram2_arr1) as len) :
                                    0 as jaccard_similarity, biblio_clean_authors) as score,
             arrayReverseSort(score)                                               as sorted_score,
             indexOf(score, sorted_score[1])                                       as matched_author_order1,
             lower(biblio_authors[matched_author_order1])                          as matched_biblio_author,
             (splitByChar(' ', matched_biblio_author) as name_segment)[-1]         as matched_biblio_author_lastname,
             concat(name_segment[-2], ' ', name_segment[-1])                       as matched_biblio_author_length_lastname
      from (
            select PaperId                    as pid,
                   orcid,
                   orcid_names,
                   arrayMap(x->x[2], authors) as authors
            from (select PaperId,
                         authors
                  from and_ds.mag_authors
                  where length(authors) == toUInt16(authors[-1][1])) any
                     inner join (select PaperId, doi, orcid, orcid_names
                                 from (select PaperId, lower(Doi) as doi from mag.paper) any
                                          inner join and_ds.orcid_author_paper_doi using doi) using PaperId
               )
      where sorted_score[1] - sorted_score[2] > 0.2
         );

create materialized view if not exists and_ds.orcid_mag_matched_paper_author_incomplete_metadata
            ENGINE = MergeTree partition by pid > 0 order by pid > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select *
from (select PaperId    as pid,
             lower(Doi) as doi,
             OriginalTitle,
             Year,
             Publisher,
             JournalId,
             OriginalVenue
      from mag.paper) any
         inner join (
    select pid,
           arraySort(x->tupleElement(x, 1),
                     groupArray((ao, aid, pkg_author_names, affiliation, affi_id))) as authors
    from (select PaperId              as pid,
                 AuthorId             as aid,
                 AffiliationId        as affi_id,
                 AuthorSequenceNumber as ao,
                 OriginalAuthor       as pkg_author_names,
                 OriginalAffiliation  as affiliation
          from mag.paper_author_affiliation) any
             inner join
         and_ds.orcid_mag_matched_paper_author using pid
    group by pid) using pid;

create table if not exists and_ds.orcid_mag_matched_paper_abstract
(
    pid      String,
    abstract String
)
    ENGINE = MergeTree partition by length(pid) > 0 order by length(pid) > 0
        settings storage_policy = 'moving_from_sda_to_sdb2';

create materialized view if not exists and_ds.orcid_mag_matched_paper_author_metadata
            ENGINE = MergeTree partition by pid > 0 order by pid > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select *
from and_ds.orcid_mag_matched_paper_author_incomplete_metadata any
         left join (select toInt64(pid) as pid, abstract from and_ds.orcid_mag_matched_paper_abstract) using pid;

create table if not exists and_ds.mag_paper_top_level_fos
(
    pid Int64,
    fos String
) ENGINE = MergeTree partition by pid > 0 order by pid > 0
      settings storage_policy = 'moving_from_sda_to_sdb2';

create materialized view if not exists and_ds.mag_paper_with_authors_ethnicity_sex_part0
            ENGINE = MergeTree partition by pid > 0 order by pid > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select pid,
       arraySort(x->toUInt16(x[1]), groupArray(author_ethnicity_sex)) as authors_ethnicity_sex_arr
from (
      select pid,
             [toString(author_position),lower(pkg_author_name), ethnic_seer, ethnea, genni, sex_mac, ssn_gender] as author_ethnicity_sex
      from and_ds.mag_author_ethnicity_sex
      where pid <= 2300000000
         )
group by pid
;

create materialized view if not exists and_ds.mag_paper_with_authors_ethnicity_sex_part1
            ENGINE = MergeTree partition by pid > 0 order by pid > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select pid,
       arraySort(x->toUInt16(x[1]), groupArray(author_ethnicity_sex)) as authors_ethnicity_sex_arr
from (
      select pid,
             [toString(author_position),lower(pkg_author_name), ethnic_seer, ethnea, genni, sex_mac, ssn_gender] as author_ethnicity_sex
      from and_ds.mag_author_ethnicity_sex
      where pid > 2300000000
         )
group by pid
;

create view if not exists and_ds.mag_paper_with_authors_ethnicity_sex as
select pid,
       arraySort(x->toUInt16(x[1]), arrayDistinct(authors_ethnicity_sex_arr)) as authors_ethnicity_sex
from (
      select *
      from and_ds.mag_paper_with_authors_ethnicity_sex_part0
      union all
      select *
      from and_ds.mag_paper_with_authors_ethnicity_sex_part1)
where length(authors_ethnicity_sex) == length(authors_ethnicity_sex_arr)
  and length(authors_ethnicity_sex_arr) == toUInt16(authors_ethnicity_sex[-1][1]);

create materialized view if not exists and_ds.materialized_mag_paper_with_authors_ethnicity_sex
            ENGINE = MergeTree partition by pid > 0 order by pid > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select *
from and_ds.mag_paper_with_authors_ethnicity_sex;
;

create materialized view if not exists and_ds.whole_mag_representativeness
            ENGINE = MergeTree partition by pid > 0 order by pid > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select PaperId                                                       as pid,
       arrayMap(x->toInt16(x[1]), authors)                           as author_positions,
       arrayMap(x->x[2], authors)                                    as author_names,
       Year                                                          as pub_year,
       lower(Doi)                                                    as doi,
       xxHash64(concat(splitByChar(' ', author_names[1])[-1], '_',
                       arrayStringConcat(extractAll(lower(OriginalTitle), '\\w+') as words_of_title,
                                         ' ') as clean_paper_title)) as pstr_id,
       length(words_of_title)                                        as num_words_of_title
from (select PaperId,
             OriginalTitle,
             Year,
             Doi
      from mag.paper
         ) any
         inner join (select PaperId,
                            authors
                     from and_ds.mag_authors
                     where length(authors) == toUInt16(authors[-1][1])) using PaperId;

create materialized view if not exists and_ds.orcid_mag_matched_paper_author_representativeness
            ENGINE = MergeTree partition by pid > 0 order by pid > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select *
from (
         select pid,
                author_position,
                author_ethnicity_sex[2] as pkg_author_name,
                author_ethnicity_sex[3] as ethnic_seer,
                author_ethnicity_sex[4] as ethnea,
                author_ethnicity_sex[5] as genni,
                author_ethnicity_sex[6] as sex_mac,
                author_ethnicity_sex[7] as ssn_gender,
                pub_year,
                orcid,
                orcid_names,
                matched_biblio_author
         from (select pid,
                      author_position,
                      authors_ethnicity_sex[author_position] as author_ethnicity_sex,
                      pub_year,
                      orcid,
                      orcid_names,
                      matched_biblio_author
               from and_ds.materialized_mag_paper_with_authors_ethnicity_sex any
                        inner join (select pid,
                                           pub_year,
                                           orcid,
                                           orcid_names,
                                           toUInt32(matched_author_order1) as author_position,
                                           matched_biblio_author
                                    from (select pid,
                                                 pub_year
                                          from and_ds.whole_mag_representativeness) any
                                             inner join and_ds.orcid_mag_matched_paper_author using pid) using pid)
         ) any
         left join (select pid, groupUniqArray(fos) as fos_arr
                    from and_ds.mag_paper_top_level_fos any
                             inner join and_ds.orcid_mag_matched_paper_author using pid
                    group by pid) using pid;
;

create materialized view if not exists and_ds.orcid_mag_matched_fullname_block
            ENGINE = MergeTree partition by length(fullname) > 0 order by length(fullname) > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select fullname,
       arrayReverseSort(x->tupleElement(x, 1),
                        groupArray((num_work, orcid, same_orcidauthor_paper_positions, lastname_variations,
                                    same_orcidauthor_paper_repres))) as full_name_blocks,
       count()                                                       as num_unique_author_inblock,
       sum(num_work)                                                 as num_citaion_in_block,
       min(num_work)                                                 as min_num_citaion_in_block,
       num_citaion_in_block / num_unique_author_inblock              as author_avg_cition_in_block,
       any(is_top100_chinese_name)                                   as is_top100_chinese_name
from (
      select orcid,
             lower((any(orcid_names) as orcid_names)[1])                        as credit_name,
             lower(orcid_names[2])                                              as lastname,
             lower(orcid_names[3])                                              as firstname,
             concat(firstname, ', ', lastname)                                  as fullname,
             count()                                                            as num_work,
             groupArray(matched_biblio_author)                                  as matched_biblio_authors,
             groupArray([pid, toInt64(author_position)])                        as same_orcidauthor_paper_positions,
             groupArray(paper_prepre)                                           as same_orcidauthor_paper_repres,
             lastname in (select lastname from and.top100_chinese_lastname)     as is_top100_chinese_name,
             arrayFilter(x->
                             not endsWith(x, lastname), matched_biblio_authors) as lastname_variations
      from (select pid,
                   author_position,
                   orcid,
                   orcid_names,
                   matched_biblio_author,
                   (pid, author_position, orcid, orcid_names, matched_biblio_author, ethnic_seer, ethnea, genni,
                    sex_mac, ssn_gender, pub_year, fos_arr) as paper_prepre
            from and_ds.orcid_mag_matched_paper_author_representativeness
            where arrayFirstIndex(x->x == 'biology', fos_arr) > 0 ? (rand(xxHash32(pid)) % 100 as rand) <= 40 :
                                                                (
                                                                        arrayFirstIndex(x->x == 'chemistry',
                                                                                        fos_arr) >
                                                                        0 ? rand <= 55:
                                                                        (arrayFirstIndex(x->x == 'medicine',
                                                                                         fos_arr) > 0
                                                                             ? rand <= 70: (arrayFirstIndex(
                                                                                                    x->x == 'physics',
                                                                                                    fos_arr) >
                                                                                            0 ? rand <= 68: 1))
                                                                    ) > 0
              and (genni == '-' ? rand % 100 <= 45 : 1) > 0
               )
      group by orcid
         )
where length(lastname) > 0
  and length(firstname) > 0
  and match(lastname, '\\w+')
  and match(firstname, '\\w+')
group by fullname
having num_unique_author_inblock >= 1
   and is_top100_chinese_name in (0, 1)
   and (num_unique_author_inblock == 1 and min_num_citaion_in_block > 1
    or num_unique_author_inblock > 1 and min_num_citaion_in_block > 0
    )
order by num_unique_author_inblock desc, num_citaion_in_block desc;


create materialized view if not exists and_ds.our_and_dataset_block
            ENGINE = MergeTree partition by length(block_fullname) > 0 order by length(block_fullname) > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select fullname                                                as block_fullname,
       author_group_orcid,
       author_group_idx_in_block,
       citation_idx_in_author_group,
       doi,
       pid,
       ao                                                      as author_position,
       tupleElement(authors[ao], 3)                            as author_name,
       tupleElement(authors[ao], 4)                            as author_affiliation,
       arrayDistinct(arrayMap(y->tupleElement(y, 3), authors)) as coauthors,
       arrayDistinct(arrayMap(y->tupleElement(y, 4), authors)) as coauthor_affliations,
       OriginalVenue                                           as venue,
       Year                                                    as pub_year,
       OriginalTitle                                           as paper_title,
       abstract                                                as paper_abstract
from (select pid,
             doi,
             OriginalTitle,
             Year,
             authors,
             OriginalVenue,
             abstract
      from and_ds.orcid_mag_matched_paper_author_metadata) any
         inner join (
    select fullname,
           author_group_idx_in_block,
           citation_idx_in_author_group,
           pid,
           ao,
           author_group_orcid
    from (
          select fullname,
                 tupleElement(
                         arrayJoin(
                                 arrayZip(arrayEnumerate(full_name_blocks), full_name_blocks)) as author_group_with_idx,
                         1)                                                              as author_group_idx_in_block,
                 tupleElement(tupleElement(author_group_with_idx, 2) as author_group,
                              1)                                                         as author_group_size,
                 tupleElement(tupleElement(author_group_with_idx, 2) as author_group, 2) as author_group_orcid,
                 tupleElement(arrayJoin(arrayZip(arrayEnumerate(pid_ao_arr),
                                                 tupleElement(author_group, 3) as pid_ao_arr)) as pid_ao_with_idx,
                              1)                                                         as citation_idx_in_author_group,
                 tupleElement(pid_ao_with_idx, 2)[1]                                     as pid,
                 tupleElement(pid_ao_with_idx, 2)[2]                                     as ao,
                 num_unique_author_inblock,
                 num_citaion_in_block
          from (select * from and_ds.orcid_mag_matched_fullname_block))
    ) using pid
order by fullname, author_group_idx_in_block, citation_idx_in_author_group
;


create materialized view if not exists and_ds.pairwise_citation_idx
            ENGINE = MergeTree partition by length(fullname) > 0 order by length(fullname) > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
with 0.32 as ratio_pos, 0.92 as ratio_neg
select fullname,
       arrayFilter(m-> rand(xxHash32(m)) % 100 <= 100 * ratio_neg,
                   arrayMap(i->
                                [
                                    [
                                        xy[2 * i + 1] as ag_idx1,
                                        toUInt32(
                                                        length(tupleElement(full_name_blocks[ag_idx1], 3))
                                                        * (rand(xxHash32(concat(
                                                            tupleElement(full_name_blocks[ag_idx1], 2),
                                                            toString(now64())))) % 100 / 100) + 1) as work_idx1,
                                        tupleElement(full_name_blocks[ag_idx1], 3)[work_idx1][1] as pid1,
                                        tupleElement(full_name_blocks[ag_idx1], 3)[work_idx1][2] as ao1
                                        ],
                                    [
                                        xy[2 * i + 2] as ag_idx2,
                                        toUInt32(
                                                        length(tupleElement(full_name_blocks[ag_idx2], 3))
                                                        * (rand(xxHash32(concat(
                                                            tupleElement(full_name_blocks[ag_idx2], 2),
                                                            toString(now64())))) % 100 / 100) + 1) as work_idx2,
                                        tupleElement(full_name_blocks[ag_idx2], 3)[work_idx2][1] as pid2,
                                        tupleElement(full_name_blocks[ag_idx2], 3)[work_idx2][2] as ao2
                                        ]
                                    ],
                            range(toUInt32(length(arrayFlatten(arrayMap(x ->
                                                                            arrayMap(y ->
                                                                                         [toUInt32(x + 1), toUInt32(y + 2)],
                                                                                     arraySlice(a, x + 1)) as b,
                                                                        range(toUInt32(length(full_name_blocks) - 1 as l)) as a) as arr) as xy) /
                                           2) as len)))      as neg_author_group_idxes,
       arrayFilter(r->r[2][2] > 0, arrayMap(n->
                                                [
                                                    [n,
                                                        (arraySort(
                                                                q->rand(xxHash32(concat(toString(q), toString(now64()))
                                                                    )),
                                                                arrayEnumerate(tupleElement(full_name_blocks[n], 3))) as sortd_idx)[1] as wid1,

                                                        tupleElement(full_name_blocks[n], 3)[wid1][1],
                                                        tupleElement(full_name_blocks[n], 3)[wid1][2]

                                                        ],
                                                    [n,
                                                        sortd_idx[2] as wid2,
                                                        tupleElement(full_name_blocks[n], 3)[wid2][1],
                                                        tupleElement(full_name_blocks[n], 3)[wid2][2]
                                                        ]
                                                    ]
           , arrayFilter(p->rand(xxHash32(concat(toString(p), toString(now64()))
                   )) % 100 <= 100 * ratio_pos,
                         arrayEnumerate(full_name_blocks)))) as pos_author_group_idxes
from and_ds.orcid_mag_matched_fullname_block
where (length(neg_author_group_idxes) > 0
    or length(pos_author_group_idxes) > 0);


create view if not exists and_ds.pairwise_citation_related_mag_paper as
select distinct(pid) as pid
from (
      select arrayJoin(arrayConcat(arrayFlatten(arrayMap(x->arrayMap(y->y[3], x), pos_author_group_idxes)),
                                   arrayFlatten(arrayMap(x->arrayMap(y->y[3], x), neg_author_group_idxes)))) as pid
      from and_ds.pairwise_citation_idx)
;

create materialized view if not exists and_ds.our_and_dataset_pairwise
            ENGINE = MergeTree partition by length(fullname) > 0 order by length(fullname) > 0
                settings storage_policy = 'moving_from_sda_to_sdb2' populate
as
select fullname,
       pid1,
       ao1,
       pid2,
       ao2,
       arrayStringConcat(
               arrayDistinct(arrayMap(y->tupleElement(y, 3), arrayFilter(z->tupleElement(z, 1) != ao1, authors1))),
               '|')                                                                                  as coauthor1,
       arrayMap(y->tupleElement(y, 2), arrayFilter(x->tupleElement(x, 1) == ao1, authors1) as a1)[1] as aid1,
       arrayMap(y->tupleElement(y, 3),
                a1)[1]                                                                               as author_names1,
       arrayStringConcat(arrayFilter(m->length(m) > 0, arrayMap(y->tupleElement(y, 4), a1)), '|')    as aff_arr1,
       arrayStringConcat(arrayFilter(n->length(n) > 0, arrayMap(y->toString(tupleElement(y, 5)), a1)),
                         '|')                                                                        as aff_id_arr1,
       paper_title1,
       replaceAll(abstract1, '\t', ' ')                                                              as abstract1,
       venue1,
       pub_year1,

       arrayStringConcat(
               arrayDistinct(arrayMap(y->tupleElement(y, 3), arrayFilter(z->tupleElement(z, 1) != ao2, authors2))),
               '|')                                                                                  as coauthor2,
       arrayMap(y->tupleElement(y, 2), arrayFilter(x->tupleElement(x, 1) == ao2, authors2) as a2)[1] as aid2,
       arrayMap(y->tupleElement(y, 3),
                a2)[1]                                                                               as author_names2,
       arrayStringConcat(arrayFilter(o->length(o) > 0, arrayMap(y->tupleElement(y, 4), a2)), '|')    as aff_arr2,
       arrayStringConcat(arrayFilter(p->length(p) > 0, arrayMap(y->toString(tupleElement(y, 5)), a2)),
                         '|')                                                                        as aff_id_arr2,
       paper_title2,
       replaceAll(abstract2, '\t', ' ')                                                              as abstract2,
       venue2,
       pub_year2,

       same_author,
       (xxHash32(concat(fullname, 'this is a random string')) % 100 as rand) <
       94 ? (rand < 70 ? -1 : 1) :
       (rand < 97 ? 0 : 2)                                                                           as train1_test0_val2
from (
         select fullname,
                pid1,
                ao1,
                pid2,
                ao2,
                same_author,
                authors1,
                paper_title1,
                abstract1,
                venue1,
                pub_year1
         from (
                  select fullname,
                         tupleElement(one_author_group, 1)[1] as pid1,
                         tupleElement(one_author_group, 1)[2] as ao1,
                         tupleElement(one_author_group, 2)[1] as pid2,
                         tupleElement(one_author_group, 2)[2] as ao2,
                         tupleElement(one_author_group, 3)    as same_author
                  from (
                        select fullname,
                               arrayJoin(arrayMap(x-> (
                                                       tupleElement(full_name_blocks[x[1][1]], 3)[x[1][2]] as pid_ao1,
                                                       tupleElement(full_name_blocks[x[2][1]], 3)[x[2][2]] as pid_ao2,
                                                       x[1][1] == x[2][1],
                                                       tupleElement(full_name_blocks[x[1][1]], 2) ==
                                                       tupleElement(full_name_blocks[x[2][1]], 2)
                                   ), arrayConcat(neg_author_group_idxes, pos_author_group_idxes))
                                   ) as one_author_group
                        from and_ds.orcid_mag_matched_fullname_block any
                                 inner join and_ds.pairwise_citation_idx using fullname))
                  any
                  left join (select pid           as pid1,
                                    authors       as authors1,
                                    OriginalTitle as paper_title1,
                                    abstract      as abstract1,
                                    OriginalVenue as venue1,
                                    Year          as pub_year1
                             from and_ds.orcid_mag_matched_paper_author_metadata any
                                      inner join and_ds.pairwise_citation_related_mag_paper using pid) using pid1) any
         left join (select pid           as pid2,
                           authors       as authors2,
                           OriginalTitle as paper_title2,
                           abstract      as abstract2,
                           OriginalVenue as venue2,
                           Year          as pub_year2
                    from and_ds.orcid_mag_matched_paper_author_metadata any
                             inner join and_ds.pairwise_citation_related_mag_paper using pid) using pid2
order by rand(xxHash64(fullname, toString(now64())))
;