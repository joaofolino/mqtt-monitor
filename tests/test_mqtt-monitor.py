import unittest
from unittest.mock import patch
from mqtt-monitor import get_disk_info

class TestMQTTMonitor(unittest.TestCase):
    @patch('mqtt-monitor.run_cmd')
    def test_get_disk_info(self, mock_run_cmd):
        mock_run_cmd.return_value = "PASSED\nTemperature: 33\nPower_On_Hours 1000\nReallocated_Sector_Ct 0"
        result = get_disk_info('/dev/sda', full=True)
        self.assertEqual(result['temperature'], '33')
        self.assertEqual(result['health'], 'OK')

if __name__ == '__main__':
    unittest.main()
