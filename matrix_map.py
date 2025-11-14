from coding_matrix import SPECIAL_TYPE_MAPPINGS, Coding_Matrix

SPECIAL_CODES = {'0K35', '024P', '067N'}

# Location codes for 67N
LOCATION_CODES_67N = {
    '029N','030N','031N','032N','033N','034N','039N','042N','045N','046N','048N',
    '055N','060N','0827','0839','0847','0850','0851','0857','0881','0882','0884',
    '0885','0886','0888','0889','0903','0951','0W17'
}

# Location codes for 97H
LOCATION_CODES_97H = {'029G', '030G', '031G'}

class MatrixMapper:
    def determine_profit_center(self, row):
        # Existing conditions
        if isinstance(row.get("Consignor"), str) and 'averitt' in row['Consignor'].lower():
            return "0004"
        if isinstance(row.get("Consignee"), str) and 'coopetrajes' in row['Consignee'].lower():
            return "0896"
        if isinstance(row.get("Consignor"), str) and 'coopetrajes' in row['Consignor'].lower():
            return "0896"
        if isinstance(row.get("Consignor"), str) and "matheson" and "fs" in row['Consignor'].lower():
            return "067N"
        if row['Consignee Code'] in SPECIAL_CODES:
            return row['Consignee Code']

        # ✅ Condition for 67N
        if row['Consignee Code'] in LOCATION_CODES_67N and isinstance(row.get("Origin Address"), str) and row['Origin Address'].startswith("570 Math"):
            return "067N"

        # ✅ Condition for 97H
        if row['Consignee Code'] in LOCATION_CODES_97H:
            return "097H"

        # Existing matrix logic
        key = (row['Consignor Type'], row['Consignee Type'])
        if key in SPECIAL_TYPE_MAPPINGS:
            return SPECIAL_TYPE_MAPPINGS[key]
        if key in Coding_Matrix:
            direction = Coding_Matrix[key]
            if direction == "ORIGIN":
                return row['Consignor Code']
            elif direction == "DESTINATION":
                return row['Consignee Code']

        return 'UNKNOWN'

