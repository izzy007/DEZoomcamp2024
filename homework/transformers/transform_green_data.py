if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    #print(data[data['trip_distance'].isin(0) & data['passenger_count'].isin(0)])

    print(data.columns)
    
    data = data[(data['trip_distance'] > 0) & (data['passenger_count'] > 0)]

    data.columns=(data.columns
        .str.replace('ID', '_id')
        .str.lower()
    )

    #         (columns={"VendorID":"vendor_id"},inplace=True)
    #print(f"this is after: {data.columns}")
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum()==0,"there are  zeros in Dataframe"

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['trip_distance'].isin([0]).sum()==0,"there are  zeros in Dataframe trip_distance"

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output ,"Camel case was not converted"
