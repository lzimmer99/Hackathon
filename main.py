# Main File for MSTR_Hackathon
import pandas as pd
from classes import MSTR
from mstrio.project_objects.datasets.cube import list_all_cubes
from mstrio.project_objects.datasets import SuperCube


def main():
    mstr = MSTR("MSTR_CONN")

    # Get all Cubes by default list_all_cubes_function
    conn = mstr.connect()
    cubes = list_all_cubes(connection=conn, to_dictionary=True)
    df_cubes = pd.DataFrame(cubes)
    cube_ids = list(df_cubes.id)

    # -----------------------------------------------------------------------------------------------#
    # Using the self created function in order to retrieve cube objects
    authToken, cookies = mstr.login()

    header = mstr.set_base_headers(authToken=authToken)

    # Iterate through all cubes
    temp_store = []
    for cube_id in cube_ids:
        result = mstr.get_cube_information(header=header,
                                           cookies=cookies,
                                           cube_id=cube_id)
        json_obj = result.json()
        cube_df = pd.json_normalize(json_obj, max_level=4)
        temp_store.append(cube_df)

    df_content = pd.concat(temp_store)

    # Match all Cube information into one dataframe
    df_content.drop("name", axis=1, inplace=True)

    df_content.rename(columns={df_content.columns[1]: "metrics",
                               df_content.columns[2]: "attributes"},
                      inplace=True)

    # Merge the Cube Data DF with metadata DF
    df_merged = pd.merge(left=df_cubes,
                         right=df_content,
                         on="id")

    # Calculate length of metric/attr-list in order to return obj count per row in df
    df_merged["attribute_count"] = df_merged['attributes'].str.len()
    df_merged["metric_count"] = df_merged['metrics'].str.len()

    # Enable Status indication - default value zero for thresholding
    df_merged["iServerCode"] = df_merged["iServerCode"].fillna(int(0))
    df_merged["code"] = df_merged["code"].fillna(int(0))
    df_merged["iServerCode"][7] = 1
    df_merged[["date_created", "date_modified"]] = df_merged[["date_created", "date_modified"]].apply(pd.to_datetime)
    print(df_merged.dtypes)
    print(df_merged.date_created)

    # Load the Cube
    ds = SuperCube(connection=conn,
                   id="7716EA7DC146EA57D101DEB8C91F21FE")

    ds.add_table(name="Governance_tbl",
                 data_frame=df_merged,
                 update_policy="replace")

    ds.update()

    conn.close()
    pass


main()
