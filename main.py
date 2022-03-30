# Main File for MSTR_Hackthon
import pandas as pd
from classes import MSTR
from mstrio.project_objects.datasets.cube import list_all_cubes


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

    # Iterate trough all cubes
    temp_store = []
    for cubeid in cube_ids:
        result = mstr.get_cube_information(header=header,
                                           cookies=cookies,
                                           cube_id=cubeid)
        json_obj = result.json()
        cube_df = pd.json_normalize(json_obj, max_level=4)
        temp_store.append(cube_df)

    df_content = pd.concat(temp_store)

    # Match all Cube information into one dataframe
    df_content.drop("name", axis=1, inplace=True)

    df_content.rename(columns={df_content.columns[1]: "metrics",
                               df_content.columns[2]: "attributes"}, inplace=True)

    df_merged = pd.merge(left=df_cubes, right=df_content, on="id")

    # Store DataFrames to csv
    df_content.to_csv("cubes.csv")
    df_cubes.to_csv("all_cubes.csv")
    df_merged.to_csv("all_cube_info.csv")

    conn.close()
    pass


if __name__ == '__main__':
    main()
