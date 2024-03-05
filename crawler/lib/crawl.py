"""
Utils to fetch and process source string
"""
import httpx

def fetch_json(url, params):
    with httpx.Client() as client:
        response = client.get(url, params=params)
        return response.json()
    

def fetch_html(url):
    """
    This function use to parse HTML or XML response to bs4 src.
    """
    with httpx.Client() as client:
        response = client.get(url)
    return response.text


def get_text_or_none(element):
    """
    Helper function to return None instead of get_text if the element is None
    """
    if element is not None:
        return element.get_text()
    else:
        return None
    
def name_getter(src, tag): 
    return src.select_one(tag).get_text()

def construct_apa_author_name(last_name, initials):
    family_name = " ".join(sur + '.' for sur in initials)
    return f'{last_name}, {family_name}'


def get_author(src):
    author_list = []
    authors = src.select("Author")
    if len(authors) == 0:
        return None
    
    for j in authors:
        try:
            last_name = name_getter(j, "LastName")
            initials = name_getter(j, "Initials")
            named = construct_apa_author_name(last_name, initials)
        except Exception:
            el = j.select_one("CollectiveName")
            if el is not None:    
                named = el.get_text()
            else:
                pass
        
        affiliate = get_text_or_none(src.select_one('Affiliation'))
        author_list.append({'author': named, 'affiliate': affiliate})
    return author_list

def get_keyword(src):
    keywords = src.select('Keyword')
    if len(keywords) == 0:
        return None
    kws = []
    for i in keywords:
        txt = get_text_or_none(i)
        kws.append(txt)

    return kws
