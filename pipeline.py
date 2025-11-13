# pipeline.py

import pandas as pd

from extract_codes import Extractor

from address_merge import CombinedAddress

from address_crossref import Merger

from map_types import TypeMapper, TypeCleaner

from matrix_map import MatrixMapper


class PipelineRunner:

    """

    FULL accrual re-coding pipeline.

    - Priority 1: Origin Type Code / Dest Type Code

    - Priority 2: Extract from Consignor / Consignee text

    - Priority 3: Combined Address fallback

    """

    def run(

        self,

        accrual_df: pd.DataFrame,

        cintas_location_table: pd.DataFrame,

        complete_location_table: pd.DataFrame,

        codes_df: pd.DataFrame,

    ) -> pd.DataFrame:

        # ----------------------------------------------------

        # 1️⃣ Normalize Consignor / Consignee text

        # ----------------------------------------------------

        extractor = Extractor()

        extractor.create_columns(accrual_df)

        extractor.lower_columns(accrual_df, "Consignor", "Consignee")

        # ----------------------------------------------------

        # 2️⃣ Priority 1: Use Origin Type Code / Dest Type Code

        # ----------------------------------------------------

        accrual_df = extractor.apply_type_code_priority(accrual_df)

        # ----------------------------------------------------

        # 3️⃣ Priority 2: Extract from text ONLY IF:

        #       - code still blank

        #       - OR consignor/consignee type = THIRD PARTY

        # ----------------------------------------------------

        accrual_df = extractor.extract_from_consignor_consignee(accrual_df, codes_df)

        # ----------------------------------------------------

        # 4️⃣ Build Combined Address (normalized)

        # ----------------------------------------------------

        comb = CombinedAddress()

        # normalize Cintas reference table

        cintas_location_table = comb.create(

            cintas_location_table,

            "Combined Address",

            "Loc_Address",

            "Loc_City",

            "Loc_ST",

        )

        # normalize accrual addresses

        accrual_df = comb.create(

            accrual_df,

            "Consignor Combined Address",

            "Origin Addresss",

            "Origin City",

            "Origin State Code",

        )

        accrual_df = comb.create(

            accrual_df,

            "Consignee Combined Address",

            "Dest Address1",

            "Dest City",

            "Dest State Code",

        )

        # ----------------------------------------------------

        # 5️⃣ Priority 3: Combined Address → fill missing codes

        # ----------------------------------------------------

        merger = Merger()

        accrual_df = merger.merge_address_codes(accrual_df, cintas_location_table)

        # ----------------------------------------------------

        # 6️⃣ Type Mapping

        # ----------------------------------------------------

        mapper = TypeMapper()

        accrual_df = mapper.map_types(

            accrual_df, cintas_location_table, "Consignor Code", "Consignor Type"

        )

        accrual_df = mapper.map_types(

            accrual_df, cintas_location_table, "Consignee Code", "Consignee Type"

        )

        cleaner = TypeCleaner()

        accrual_df = cleaner.fill_non_cintas(

            accrual_df, "Consignor Type", "Consignee Type"

        )

        # ----------------------------------------------------

        # 7️⃣ Matrix Mapping → Assigned Location Code

        # ----------------------------------------------------

        matrix_mapper = MatrixMapper()

        accrual_df["Assigned Location Code"] = accrual_df.apply(

            matrix_mapper.determine_profit_center, axis=1

        )

        # ----------------------------------------------------

        # 8️⃣ Merge Profit / Cost Center EJ

        # ----------------------------------------------------

        accrual_df = accrual_df.merge(

            complete_location_table[["Loc Code", "Prof_Cntr", "Cost_Cntr"]],

            left_on="Assigned Location Code",

            right_on="Loc Code",

            how="left",

        )

        accrual_df.rename(

            columns={"Prof_Cntr": "Profit Center EJ", "Cost_Cntr": "Cost Center EJ"},

            inplace=True,

        )

        # ----------------------------------------------------

        # 9️⃣ Account EJ Rule

        # ----------------------------------------------------

        accrual_df["Account # EJ"] = accrual_df.apply(

            lambda row: (

                621000

                if row.get("Consignee Code") == row.get("Assigned Location Code")

                else 621020

            ),

            axis=1,

        )

        return accrual_df
 
