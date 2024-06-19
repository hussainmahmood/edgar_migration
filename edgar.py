# coding: utf-8
from sqlalchemy import Column, Date, DateTime, Float, Integer, LargeBinary, Numeric, Unicode
from sqlalchemy.dialects.mssql import BIT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Context(Base):
    __tablename__ = 'context'

    context_id = Column(Integer, primary_key=True)
    accession_id = Column(Integer, nullable=False)
    period_start = Column(Date)
    period_end = Column(Date)
    period_instant = Column(Date)
    specifies_dimensions = Column(BIT, nullable=False)
    context_xml_id = Column(Unicode, nullable=False)
    entity_scheme = Column(Unicode, nullable=False)
    entity_identifier = Column(Unicode, nullable=False)
    fiscal_year = Column(Integer)
    fiscal_period = Column(Unicode)
    context_hash = Column(LargeBinary)
    dimension_hash = Column(LargeBinary)
    calendar_year = Column(Integer)
    calendar_period = Column(Unicode)
    calendar_start_offset = Column(Numeric(19, 7))
    calendar_end_offset = Column(Numeric(19, 7))
    calendar_period_size_diff_percentage = Column(Float(53))
    dimension_count = Column(Integer)


class ContextDimensionExplicit(Base):
    __tablename__ = 'context_dimension_explicit'

    context_dimension_id = Column(Integer, primary_key=True)
    context_id = Column(Integer, nullable=False)
    dimension_qname_id = Column(Integer, nullable=False)
    member_qname_id = Column(Integer)
    typed_qname_id = Column(Integer)
    is_default = Column(BIT, nullable=False)
    is_segment = Column(BIT)
    typed_text_content = Column(Unicode)
    dimension_namespace = Column(Unicode)
    dimension_local_name = Column(Unicode)
    member_namespace = Column(Unicode)
    member_local_name = Column(Unicode)


class Fact(Base):
    __tablename__ = 'fact'

    fact_id = Column(Integer, primary_key=True)
    accession_id = Column(Integer, nullable=False)
    context_id = Column(Integer)
    unit_id = Column(Integer)
    unit_base_id = Column(Integer)
    element_id = Column(Integer, nullable=False)
    fact_value = Column(Unicode)
    xml_id = Column(Unicode)
    precision_value = Column(Integer)
    decimals_value = Column(Integer)
    is_precision_infinity = Column(BIT, nullable=False)
    is_decimals_infinity = Column(BIT, nullable=False)
    ultimus_index = Column(Integer)
    calendar_ultimus_index = Column(Integer)
    fiscal_ultimus_index = Column(Integer)
    uom = Column(Unicode)
    is_extended = Column(BIT)
    fiscal_year = Column(Integer)
    fiscal_period = Column(Unicode)
    calendar_year = Column(Integer)
    calendar_period = Column(Unicode)
    tuple_fact_id = Column(Integer)
    fact_hash = Column(LargeBinary)
    calendar_hash = Column(LargeBinary)
    fiscal_hash = Column(LargeBinary)
    entity_id = Column(Integer)
    element_namespace = Column(Unicode)
    element_local_name = Column(Unicode)
    dimension_count = Column(Integer)
    inline_display_value = Column(Unicode)
    inline_scale = Column(Integer)
    inline_negated = Column(BIT)
    inline_is_hidden = Column(BIT)
    inline_format_qname_id = Column(Integer)


class Report(Base):
    __tablename__ = 'report'

    report_id = Column(Integer, primary_key=True)
    source_id = Column(Integer, nullable=False)
    entity_id = Column(Integer, nullable=False)
    source_report_identifier = Column(Unicode)
    dts_id = Column(Integer, nullable=False)
    entry_dts_id = Column(Integer)
    creation_timestamp = Column(DateTime, nullable=False)
    accepted_timestamp = Column(DateTime, nullable=False)
    is_most_current = Column(BIT, nullable=False)
    entity_name = Column(Unicode)
    creation_software = Column(Unicode)
    entry_type = Column(Unicode, nullable=False)
    entry_url = Column(Unicode, nullable=False)
    entry_document_id = Column(Integer, nullable=False)
    alternative_document_id = Column(Integer)
    reporting_period_end_date = Column(DateTime)
    restatement_index = Column(Integer)
    period_index = Column(Integer)
    properties = Column(Unicode)
    documentset_num = Column(Integer)
