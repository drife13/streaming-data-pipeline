package com.labs1904.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._

import java.time.LocalDate
import scala.reflect.runtime.{universe => ru}

object Util {
	def constructSchema[T: ru.TypeTag](): StructType = {
		ScalaReflection
			.schemaFor[T]
			.dataType
			.asInstanceOf[StructType]
	}

	def schemaValidationFilter(schema: StructType): Column = {
		var filter: Column = null

		schema.foreach { field =>
			val fieldName = field.name
			val isNullable = field.nullable
			val dataType = field.dataType

			if (!isNullable) {
				val newFilter = col(fieldName).isNotNull
				filter = combineFilters(filter, newFilter)
			}

			dataType match {
				case DateType => filter = combineFilters(filter, validateDateUDF(col(fieldName)))
				case _ =>
			}
		}

		filter
	}

	def combineFilters(filter: Column, newFilter: Column): Column = {
		var combinedFilter = filter
		if (combinedFilter == null) {
			combinedFilter = newFilter
		} else {
			combinedFilter = combinedFilter && newFilter
		}
		combinedFilter
	}

	val validateDateUDF = udf((dateString: String) => {
		// val dateFormat = "yyyy-MM-dd" // Modify this according to your date format
		// val formatter = DateTimeFormatter.ofPattern(dateFormat)
		try {
			// LocalDate.parse(dateString, formatter)
			LocalDate.parse(dateString)
			true // Date is valid
		} catch {
			case _: Exception => false // Date is invalid
		}
	})

	def iterableOfFieldNames[T: ru.TypeTag](): Iterable[String] = {
		val tpe = ru.typeOf[T]
		tpe.decls.collect {
			case m: ru.MethodSymbol if m.isCaseAccessor =>
				m.name.toString
		}
	}

	def getTypeString[T: ru.TypeTag](fieldName: String): Option[String] = {
		val tpe = ru.typeOf[T]
		val fieldSymbol = tpe.decl(ru.TermName(fieldName))

		if (fieldSymbol.isMethod && fieldSymbol.asMethod.isCaseAccessor) {
			Some(fieldSymbol.typeSignature.resultType.toString)
		} else {
			None
		}
	}
}
