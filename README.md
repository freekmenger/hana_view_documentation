# hana_view_documentation
Generate HANA Calculation view documentation (from target to source, column-based, including calculations).

Instructions:
- download hana calculation views, either on folder level & unzip or seperately. Save all .hdbcalcview files in one folder
- open a python session and add the password in keyring for the HANA user for the system you want to load the definitions from. E.g. 
  keyring.set_password(hana-production.domain.com, "password", "init1234")
- run this program with python. Check all parameters are correct, not only the required ones. E.g. python hana_view_documentation.py --host hana-production.domain.com....
- the program will generate an excel file with one sheet per calculation view

Parameters:
- host: Hostname of the HANA server
- port: Port of the HANA server, default: 30015
- user: User for the HANA server - needs appriopriate read-rights on the HDB module schema, default: SYSTEM
- schema: Schema where the calculation views are stored, default: HDB_DATAMART
- calcviewdir: Calculation view directory. Export calculation view folder, unzip to a directory. Add that directory to this parameter.
- output_file: Output file. Make sure to include the complete path and file with extension: xlsx.

Tested against HANA version: SPS 4 Patch 5. In theory it should work with other versions of HANA.
