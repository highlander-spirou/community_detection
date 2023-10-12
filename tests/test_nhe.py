import pytest
from pydantic import BaseModel, field_validator
from hh.parser import get_pmid_info, get_pmid_list


class BaseValidator(BaseModel):
    title: str
    authors: list
    keywords: list
    journal: str
    journal_abrev: str
    volume: str
    journal_issue: str
    year: str
    month: str
    doi: str
    reference_id_list: list
    reference_num: int
    num_cite: str

    @field_validator("authors")
    @classmethod
    def validate_author_field(cls, value):
        for i in value:
            if 'last_name' not in i \
                    or 'fore_name' not in i \
                    or 'initials' not in i \
                    or 'apa_name' not in i:
                raise ValueError("Author field invalid")
        return value


@pytest.mark.asyncio
async def test_get_pmid_list():
    LIMIT = 5
    OFFSET = 0
    pmid_list = await get_pmid_list(LIMIT, OFFSET)
    assert len(pmid_list) == 5


@pytest.mark.asyncio
async def test_pmid_info():
    pmid = 31341288
    result = await get_pmid_info(pmid)

    BaseValidator(**result)


@pytest.mark.asyncio
async def test_run_crawler_overall():
    LIMIT = 5
    OFFSET = 0
    pmid_list = await get_pmid_list(LIMIT, OFFSET)
    for i in pmid_list:
        result = await get_pmid_info(i)
        BaseValidator(**result)

