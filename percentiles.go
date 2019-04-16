package wrench

// Percentiles is a list of percentiles to include in a latency distribution,
// e.g. 10.0, 50.0, 99.0, 99.99, etc.
type Percentiles []float64

// Logarithmic percentile scale.
var Logarithmic = Percentiles{
	0.0,
	1.0,
	2.0,
	3.0,
	4.0,
	5.0,
	6.0,
	7.0,
	8.0,
	9.0,
	10.0,
	11.0,
	12.0,
	13.0,
	14.0,
	15.0,
	16.0,
	17.0,
	18.0,
	19.0,
	20.0,
	21.0,
	22.0,
	23.0,
	24.0,
	25.0,
	26.0,
	27.0,
	28.0,
	29.0,
	30.0,
	31.0,
	32.0,
	33.0,
	34.0,
	35.0,
	36.0,
	37.0,
	38.0,
	39.0,
	40.0,
	41.0,
	42.0,
	43.0,
	44.0,
	45.0,
	46.0,
	47.0,
	48.0,
	49.0,
	50.0,
	51.0,
	52.0,
	53.0,
	54.0,
	55.0,
	56.0,
	57.0,
	58.0,
	59.0,
	60.0,
	61.0,
	62.0,
	63.0,
	64.0,
	65.0,
	66.0,
	67.0,
	68.0,
	69.0,
	70.0,
	71.0,
	72.0,
	73.0,
	74.0,
	75.0,
	76.0,
	77.0,
	78.0,
	79.0,
	80.0,
	81.0,
	82.0,
	83.0,
	84.0,
	85.0,
	86.0,
	87.0,
	88.0,
	89.0,
	90.0,
	91.0,
	92.0,
	93.0,
	94.0,
	95.0,
	95.0,
	95.625,
	96.25,
	96.875,
	97.1875,
	97.5,
	97.8125,
	98.125,
	98.4375,
	98.5938,
	98.75,
	98.9062,
	99.0625,
	99.2188,
	99.2969,
	99.375,
	99.4531,
	99.5313,
	99.6094,
	99.6484,
	99.6875,
	99.7266,
	99.7656,
	99.8047,
	99.8242,
	99.8437,
	99.8633,
	99.8828,
	99.9023,
	99.9121,
	99.9219,
	99.9316,
	99.9414,
	99.9512,
	99.9561,
	99.9609,
	99.9658,
	99.9707,
	99.9756,
	99.978,
	99.9805,
	99.9829,
	99.9854,
	99.9878,
	99.989,
	99.9902,
	99.9915,
	99.9927,
	99.9939,
	99.9945,
	99.9951,
	99.9957,
	99.9963,
	99.9969,
	99.9973,
	99.9976,
	99.9979,
	99.9982,
	99.9985,
	99.9986,
	99.9988,
	99.9989,
	99.9991,
	99.9992,
	99.9993,
	99.9994,
	99.9995,
	99.9996,
	99.9997,
	99.9998,
	99.9999,
	100.0,
}