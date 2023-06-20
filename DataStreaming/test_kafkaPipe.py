import pytest
from unittest import mock
from DataStreaming import kafkaPipe


def test_CheckInput():
    """
    Test that it uses default for negative numbers
    Test that it uses default for not numbers
    Test that it uses rounding for floats
    Test that it uses default for zero
    Test that it uses correct for positive numbers
    """
    assert(kafkaPipe.checkInput(-1, 10, "testVal") == 10)
    assert(kafkaPipe.checkInput("abc", 10, "testVal") == 10)
    assert(kafkaPipe.checkInput(5.14, 10, "testVal") == 5)
    assert(kafkaPipe.checkInput(0, 10, "testVal") == 10)
    assert(kafkaPipe.checkInput(5, 10, "testVal") == 5)

@mock.patch("kafkaPipe.os.path")
@mock.patch("kafkaPipe.os")
def test_folder_create(mock_os,mock_path):
    """
    Test that "Data" folder created if doesn't exist
    """
    mock_path.isdir.return_value = False
    mock_path.exists.return_value = False
    kafkaPipe.folderInit()
    mock_os.makedirs.assert_called_with("Data")

@mock.patch("kafkaPipe.os.path")
def test_folder_error(mock_path):
    """
    Test that error raised if "Data" exists but is file
    """
    mock_path.isdir.return_value = False
    mock_path.exists.return_value = True
    with pytest.raises(RuntimeError) as exc_info:
        kafkaPipe.folderInit()
    assert(str(exc_info.value) == "File called Data already exists")

def test_fileInitTxt():
    """
    Test that opens file if not csv
    """
    with mock.patch('kafkaPipe.open', mock.mock_open()) as m:
        csvList = kafkaPipe.fileInit("bad.txt", [])
        m.assert_called_once_with("bad.txt", "a")
        assert(len(csvList) == 1)

@mock.patch('kafkaPipe.csv.writer')
@mock.patch("kafkaPipe.os.path")
def test_fileInitCSVEmpty(mock_path,mock_writer):
    """
    Test that writes header if csv empty
    """
    mock_path.getsize.return_value = 0
    with mock.patch('kafkaPipe.open', mock.mock_open()) as m:
        csvList = kafkaPipe.fileInit("test.csv", ["1", "2"])
        m.assert_called_once_with("test.csv","a")
        assert(len(csvList) == 2)
        assert(mock_writer.called)
        #mock() means the object created by that function
        mock_writer().writerow.assert_called_once_with(["1","2"])

@mock.patch('kafkaPipe.csv.writer')
@mock.patch("kafkaPipe.os.path")
def test_fileInitCSV(mock_path,mock_writer):
    """
    Test that doesn't writes header if csv not empty
    """
    mock_path.getsize.return_value = 1
    with mock.patch('kafkaPipe.open', mock.mock_open()) as m:
        csvList = kafkaPipe.fileInit("test.csv", ["1", "2"])
        m.assert_called_once_with("test.csv","a")
        assert(len(csvList) == 2)
        assert(mock_writer.called)
        assert(not mock_writer().writerow.called)

@mock.patch("kafkaPipe.Popen")
def test_consumeInitTunnel(mock_popen):
    """
    Test that error raised if tunnel not open
    """
    mock_popen().communicate.return_value = (b'',None)
    with pytest.raises(RuntimeError) as exc_info:
        kafkaPipe.consumeInit()
        assert(mock_popen.called)
        assert(str(exc_info.value) == "local tunnel not open")

@mock.patch("kafkaPipe.KafkaConsumer")
@mock.patch("kafkaPipe.Popen")
def test_consumeInitBootstrap(mock_popen,mock_consume):
    """
    Test that error raised if consumer not connected
    """
    mock_popen().communicate.return_value = (b'123\n',None)
    mock_consume().bootstrap_connected.return_value = False
    with pytest.raises(RuntimeError) as exc_info:
        kafkaPipe.consumeInit()
        assert(mock_popen.called)
        assert(mock_consume.called)
        assert(str(exc_info.value) == "Bootstrap not connected")

def test_offsetInitLarger():
    """
    Test that init offset replaces current offset if larger 
    """
    mock_obj = mock.MagicMock()
    mock_obj.beginning_offsets.return_value = {0:10,1:20}
    part = [0,1]
    offs = [0,0]
    result = kafkaPipe.offsetInit(mock_obj, part, offs)
    assert(result == [10,20])
    mock_obj.seek.assert_has_calls([mock.call(0,10),mock.call(1,20)])

def test_offsetInitSmaller():
    """
    Test that init offset doesn't replace current offset if smaller 
    """
    mock_obj = mock.MagicMock()
    mock_obj.beginning_offsets.return_value = {0:0,1:0}
    part = [0,1]
    offs = [10,20]
    result = kafkaPipe.offsetInit(mock_obj, part, offs)
    assert(result == [10,20])
    mock_obj.seek.assert_has_calls([mock.call(0,10),mock.call(1,20)])

def test_writeMsgGoodmpg():
    """
    Test a good message write for mpg.csv
    """
    mock_obj1 = mock.Mock()
    mock_obj2 = mock.Mock()
    mock_obj3 = mock.Mock()
    mock_obj4 = mock.Mock()
    input = "2023-03-20T08:13,722141,GET /data/m/schindlers+list+1993/154.mpg"
    kafkaPipe.writeMsg(input, mock_obj1, mock_obj2, mock_obj3, mock_obj4)
    mock_obj1.writerow.assert_called_with(["2023-03-20T08:13","722141","GET /data/m/schindlers+list+1993/154.mpg"])

def test_writeMsgBadmpg1():
    """
    Test a bad message with too many commas for mpg.csv
    """
    mock_obj1 = mock.Mock()
    mock_obj2 = mock.Mock()
    mock_obj3 = mock.Mock()
    mock_obj4 = mock.Mock()
    input = "2023-03-20T08:13,722141,GET /data/m/schindlers+list+1993/,154.mpg"
    kafkaPipe.writeMsg(input, mock_obj1, mock_obj2, mock_obj3, mock_obj4)
    assert(mock_obj4.write.called)

def test_writeMsgBadmpg2():
    """
    Test a bad message with user id too low for mpg.csv
    """
    mock_obj1 = mock.Mock()
    mock_obj2 = mock.Mock()
    mock_obj3 = mock.Mock()
    mock_obj4 = mock.Mock()
    input = "2023-03-20T08:13,0,GET /data/m/schindlers+list+1993/154.mpg"
    kafkaPipe.writeMsg(input, mock_obj1, mock_obj2, mock_obj3, mock_obj4)
    assert(mock_obj4.write.called)

def test_writeMsgGoodrecs():
    """
    Test a good message write for recs.csv
    """
    mock_obj1 = mock.Mock()
    mock_obj2 = mock.Mock()
    mock_obj3 = mock.Mock()
    mock_obj4 = mock.Mock()
    input = "2023-03-20T08:12:38.176557,128017,recommendation request 17645-team14.isri.cmu.edu:8082, status 200, result: the+green+mile+1999, the+sound+of+music+1965, 131 ms"
    kafkaPipe.writeMsg(input, mock_obj1, mock_obj2, mock_obj3, mock_obj4)
    mock_obj2.writerow.assert_called_with(['2023-03-20T08:12:38.176557', '128017', 'recommendation request 17645-team14.isri.cmu.edu:8082', ' result: the+green+mile+1999, the+sound+of+music+1965', ' 131 ms'])

def test_writeMsgGoodrecs():
    """
    Test a bad message write with bad status for recs.csv
    """
    mock_obj1 = mock.Mock()
    mock_obj2 = mock.Mock()
    mock_obj3 = mock.Mock()
    mock_obj4 = mock.Mock()
    input = "2023-03-20T08:12:38.176557,128017,recommendation request 17645-team14.isri.cmu.edu:8082, status 400, result: the+green+mile+1999, the+sound+of+music+1965, 131 ms"
    kafkaPipe.writeMsg(input, mock_obj1, mock_obj2, mock_obj3, mock_obj4)
    assert(mock_obj4.write.called)

def test_writeMsgGoodrate():
    """
    Test a good message write for rate.csv
    """
    mock_obj1 = mock.Mock()
    mock_obj2 = mock.Mock()
    mock_obj3 = mock.Mock()
    mock_obj4 = mock.Mock()
    input = "2023-03-20T08:12:57,287206,GET /rate/titanic+1997=3"
    kafkaPipe.writeMsg(input, mock_obj1, mock_obj2, mock_obj3, mock_obj4)
    mock_obj3.writerow.assert_called_with(["2023-03-20T08:12:57","287206","GET /rate/titanic+1997=3"])

def test_writeMsgBadrate():
    """
    Test a bad message write with user id too high for rate.csv
    """
    mock_obj1 = mock.Mock()
    mock_obj2 = mock.Mock()
    mock_obj3 = mock.Mock()
    mock_obj4 = mock.Mock()
    input = "2023-03-20T08:12:57,1000001,GET /rate/titanic+1997=3"
    kafkaPipe.writeMsg(input, mock_obj1, mock_obj2, mock_obj3, mock_obj4)
    assert(mock_obj4.write.called)

def test_writeMsgBad():
    """
    Test a bad message that matches none
    """
    mock_obj1 = mock.Mock()
    mock_obj2 = mock.Mock()
    mock_obj3 = mock.Mock()
    mock_obj4 = mock.Mock()
    input = "2023-03-20T08:12:57,1000001,GET /rate /titanic+1997=3"
    kafkaPipe.writeMsg(input, mock_obj1, mock_obj2, mock_obj3, mock_obj4)
    assert(mock_obj4.write.called)

def test_closeFiles():
    """
    Test that closing files called correctly
    """
    mock_obj1 = mock.Mock()
    mock_obj2 = mock.Mock()
    mock_obj3 = mock.Mock()
    mock_obj4 = mock.Mock()
    kafkaPipe.closeFiles(mock_obj1, mock_obj2, mock_obj3, mock_obj4)
    assert(mock_obj1.close.called)
    assert(mock_obj2.close.called)
    assert(mock_obj3.close.called)
    assert(mock_obj4.close.called)

def test_writeOffsets():
    """
    Test that writeOffsets opens and calls correct functions
    """
    content = [1,2]
    with mock.patch('kafkaPipe.open', mock.mock_open()) as mocked_file:
        kafkaPipe.writeOffsets(content)
        mocked_file.assert_called_once_with("Offsets.py", 'w')
        mocked_file().write.assert_has_calls([mock.call("PART0OFF=" + "%d\n" % content[0]),
                                              mock.call("PART1OFF=" + "%d\n" % content[1])])