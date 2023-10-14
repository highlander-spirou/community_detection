import httpx
import bs4
from hh.parser.utils import get_text_or_none, get_author_variants

def fetch_json_sync(url, params):
    with httpx.Client() as client:
        response = client.get(url, params=params)
        return response.json()
    
def fetch_html_sync(url):
    with httpx.Client() as client:
        response = client.get(url)
    return bs4.BeautifulSoup(response.text, features="xml")

def get_cited_num_sync(pmid):
    cited_url = f'https://pubmed.ncbi.nlm.nih.gov/?linkname=pubmed_pubmed_citedin&from_uid={pmid}'
    src =fetch_html_sync(cited_url)
    num_cite = get_text_or_none(src.select_one('div.results-amount > h3 > span'))
    return num_cite


def get_pmid_info_sync(pmid):
    """
    Extract useful information
    """
    root = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
    src = fetch_html_sync(root)

    # Attribute getters
    title = get_text_or_none(src.select_one("ArticleTitle"))
    if title is None:
        raise Exception('Title not found')
    authors = get_author_variants(src)
    if len(authors) == 0:
        raise Exception(f'Cannot fetch author for {pmid}', 'error')
    keywords = src.select('Keyword')
    if len(keywords) > 0:
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
    num_cite = get_cited_num_sync(pmid)


    return dict(title=title, authors=authors, keywords=keywords, journal=journal,
                journal_abrev=journal_abrev, volume=volume, journal_issue=journal_issue,
                year=year, month=month, page=page, doi=doi, reference_id_list=reference_id_list,
                reference_num=reference_num, num_cite=num_cite)

