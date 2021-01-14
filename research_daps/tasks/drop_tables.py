from daps_utils.db import db_session, CALLER_PKG
import research_daps.orms.companies_house as orms
# import research_daps.orms.glass as orms

print(CALLER_PKG)
with db_session(database="production") as session:
    engine = session.get_bind()
    orms.Base.metadata.drop_all(engine)

    # print(
    #     session.query(orms.Address)
    #     .filter(
    #         orms.Address.address_text.in_(
    #             [
    #                 "74 George street Luton LU1 2BD",
    #             ]
    #         )
    #     )
    #     .all()
    # )
