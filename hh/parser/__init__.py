from hh.parser.utils import fetch_json, fetch_html, get_author_variants, get_cited_num, get_text_or_none


async def get_pmid_list(limit, offset):
    """
    Parse all pubmed PMID that match term `biomedical data science`
    """
    root_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
    params = {
        'db': 'pubmed',
        'retmode': 'json',
        'retmax': limit,
        'retstart': offset,
        'sort': 'relevance',
        'term': 'biomedical data science'
    }
    response = await fetch_json(root_url, params)
    idlist = response['esearchresult']['idlist']
    return idlist


async def get_pmid_info(pmid):
    """
    Extract useful information
    """
    root = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
    src = await fetch_html(root)

    # Attribute getters
    try:
        title = get_text_or_none(src.select_one("ArticleTitle"))
        authors = get_author_variants(src)
        keywords = src.select('Keyword')
        if keywords is not None:
            keywords = [get_text_or_none(i) for i in keywords]
        journal = get_text_or_none(src.select_one('Title'))
        journal_abrev = get_text_or_none(src.select_one('ISOAbbreviation'))
        volume = get_text_or_none(src.select_one('Volume'))
        journal_issue = get_text_or_none(src.select_one('Issue'))
        year = get_text_or_none(src.select_one('Year'))
        month = get_text_or_none(src.select_one('Month'))
        page = get_text_or_none(src.select_one('MedlinePgn'))
        doi = get_text_or_none(src.select_one('ELocationID[EIdType="doi"]'))
        reference_id_list =  src.select('Reference > ArticleIdList > ArticleId[IdType="pubmed"]')
        if reference_id_list is not None:
            reference_id_list = [get_text_or_none(i) for i in reference_id_list]
        reference_num = len([i for i in reference_id_list if i is not None])
        num_cite = await get_cited_num(pmid)

    except Exception as e:
        raise UserWarning(f'Error when parsing {pmid}')

    return dict(title=title, authors=authors, keywords=keywords, journal=journal,
                journal_abrev=journal_abrev, volume=volume, journal_issue=journal_issue,
                year=year, month=month, page=page, doi=doi, reference_id_list=reference_id_list,
                reference_num=reference_num, num_cite=num_cite)
