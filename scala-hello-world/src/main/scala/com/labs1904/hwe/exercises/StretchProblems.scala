package com.labs1904.hwe.exercises

object StretchProblems {

	/*
	Checks if a string is palindrome.
 */
	def isPalindrome(s: String): Boolean = {
		s == s.reverse
	}

	/*
For a given number, return the next largest number that can be created by rearranging that number's digits.
If no larger number can be created, return -1
 */
	def getNextBiggestNumber(i: Integer): Int = {
		val numArray = i.toString.toCharArray

		val numIndexedSeq = numArray.zipWithIndex

		val lastIndex = numArray.size - 1

		val pivotIndex = numIndexedSeq
			.indexWhere {
				case (_: Char, i: Int) =>
					i == lastIndex || numArray
						.takeRight(numArray.size - i)
						.sliding(2)
						.forall((a: Array[Char]) => a(0) >= a(1))
			} - 1

		if (pivotIndex < 0) {
			return -1
		}

		val pivotVal = numArray(pivotIndex)

		val lastSuccessorIndex = numIndexedSeq
			.lastIndexWhere {
				case (num: Char, i: Int) => i > pivotIndex && num > pivotVal
			}

		if (lastSuccessorIndex < 0) {
			return -1
		}

		val successorVal = numArray(lastSuccessorIndex)

		numArray(lastSuccessorIndex) = pivotVal
		numArray(pivotIndex) = successorVal

		val firstSlice = numArray
			.slice(0, pivotIndex + 1)

		val reverseSlice = numArray
			.slice(pivotIndex + 1, numArray.size)
			.reverse

		val newCharArray = Array.concat(firstSlice, reverseSlice)

		String.valueOf(newCharArray).toInt
	}

}
