import psycopg2
import psycopg2.errorcodes

# Constants
DB_NAME = 'news'
POPULAR_ARTICLES_COUNT = 3
ERR_PERCENTAGE = 1


def view_author_title_slug():
    """
    Creates a view of author name, title and the slug
    """

    try:
        # Create DB connection
        db = psycopg2.connect(database=DB_NAME)
        c = db.cursor()
        try:
            # Execute the query
            c.execute("create view author_title_slug as "
                      "select articles.title, authors.name, articles.slug "
                      "from articles join authors "
                      "on articles.author = authors.id")
        except Exception, e:  # Exception on execute
            db.close()
            # print ("View already exists!")
    except Exception, e:  # Exception on DB connection
        print (psycopg2.errorcodes.lookup(e.pgcode))


def report_popular_articles():
    """
    Question 1. What are the most popular three articles of all time?

    Take away point:
    Which articles have been accessed the most?

    Output:
    Present this information as a sorted list with the most popular article
    at the top.
    """

    try:
        # Create DB connection
        db = psycopg2.connect(database=DB_NAME)
        c = db.cursor()
        try:
            # Execute the query
            c.execute(
                "select author_title_slug.title, "
                "count(author_title_slug.title) as views from "
                "author_title_slug left join log "
                "on log.path like concat('%', author_title_slug.slug, '%') "
                "group by author_title_slug.title order by views desc "
                "limit {value}".format(value=POPULAR_ARTICLES_COUNT))
            return c.fetchall()
        except Exception, e:  # Exception on execute
            db.close()
            print (psycopg2.errorcodes.lookup(e.pgcode))
    except Exception, e:  # Exception on DB connection
        print (psycopg2.errorcodes.lookup(e.pgcode))


def report_popular_article_authors():
    """
    Question 2. Who are the most popular article authors of all time?

    Take away point:
    When you sum up all of the articles each author has written,
    which authors get the most page views?

    Output:
    Present this as a sorted list with the most popular author at the top.
    """

    try:
        # Create DB connection
        db = psycopg2.connect(database=DB_NAME)
        c = db.cursor()
        try:
            # Execute the query
            c.execute(
                "select author_title_slug.name, count(author_title_slug.name) "
                "as views "
                "from author_title_slug left join log "
                "on log.path like concat('%', author_title_slug.slug, '%') "
                "group by author_title_slug.name order by views desc")
            return c.fetchall()
        except Exception, e:  # Exception on execute
            db.close()
            print (psycopg2.errorcodes.lookup(e.pgcode))
    except Exception, e:  # Exception on DB connection
        print (psycopg2.errorcodes.lookup(e.pgcode))


def report_error_days():
    """
    Question 3. On which days did more than 1% of requests lead to errors?

    Take away point:
    The log table includes a column status that indicates the HTTP status code
    that the news site sent to the user's browser.

    Output:
    Day and the error percentage
    """

    try:
        # Create DB connection
        db = psycopg2.connect(database=DB_NAME)
        c = db.cursor()
        try:
            # Execute the query
            c.execute("select trim(to_char(final.now_date, 'Month')) || ' ' "
                      "|| to_char(final.now_date,'dd, yyyy') as day, "
                      "final.err_perc from "
                      "(select all_status.now_date, "
                      "round((100.0 * error_status.err_count"
                      "/all_status.tot_count), 2) as err_perc from "
                      "(select cast(log.time as date) as now_date, count(*) "
                      "as tot_count "
                      "from log group by now_date) as all_status "
                      "join "
                      " (select cast(log.time as date) as now_date, count(*) "
                      "as err_count "
                      "from log where status like '4%' group by now_date) "
                      "as error_status "
                      "on all_status.now_date = error_status.now_date "
                      "order by err_perc desc) as final "
                      "where final.err_perc > {value}".
                      format(value=ERR_PERCENTAGE))
            return c.fetchall()
        except Exception, e:  # Exception on execute
            db.close()
            print (psycopg2.errorcodes.lookup(e.pgcode))
    except Exception, e:  # Exception on DB connection
        print (psycopg2.errorcodes.lookup(e.pgcode))


def main():
    """
    Main function
    """

    print ("\n**********	Welcome to the Reporting Tool!	**********\n")

    # Create the required view
    view_author_title_slug()

    # Question 1
    print ("Finding " + str(POPULAR_ARTICLES_COUNT) +
           " most popular articles . . . ")
    popular_articles = report_popular_articles()
    str_text = str(POPULAR_ARTICLES_COUNT) + " most popular articles are: \n"
    for article in popular_articles:
        str_text = str_text + u'\u2022' + " \"" + article[0] + "\" - " + str(
            article[1]) + " views \n"

    # Question 2
    print ("Finding most popular article authors of all time . . . ")
    popular_article_authors = report_popular_article_authors()
    str_text = str_text + "\nMost popular article authors of all time are: \n"
    for author in popular_article_authors:
        str_text = str_text + u'\u2022' + " " + author[0] + " - " + str(
            author[1]) + " views \n"

    # Question 3
    print ("Finding days on which more than " + str(
        ERR_PERCENTAGE) + "% requests led to errors . . . ")
    error_days = report_error_days()
    str_text = str_text + "\nDays on which more than " + str(
        ERR_PERCENTAGE) + "% requests led to errors are: \n"
    for day in error_days:
        str_text = str_text + u'\u2022' + " " + day[0] + " - " + str(
            day[1]) + "% errors \n"

    # Store the output in a text file
    print ("Writing output to file Report.txt . . . ")
    text_file = open("Report.txt", "w")
    text_file.write(str_text.encode('utf-8'))
    text_file.close()
    print ("Done!")


if __name__ == "__main__":
    main()
