package com.labs1904.spark.classes

import java.sql.Date

case class Review(
									 marketplace: String,
									 customer_id: Int,
									 review_id: String,
									 product_id: String,
									 product_parent: Int,
									 product_title: String,
									 product_category: String,
									 star_rating: Int,
									 helpful_votes: Int,
									 total_votes: Int,
									 vine: String,
									 verified_purchase: String,
									 review_headline: String,
									 review_body: String,
									 review_date: Date
								 )

case class User(
								 mail: String,
								 birthdate: Date,
								 name: String,
								 sex: String,
								 username: String
							 )

case class UserReview(
											 user: User,
											 review: Review
										 )
