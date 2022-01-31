import { Database } from "../src/database";
import { ACTORS, DIRECTORS, GENRES, KEYWORDS, MOVIES, MOVIE_ACTORS, MOVIE_DIRECTORS, MOVIE_GENRES, MOVIE_KEYWORDS, MOVIE_RATINGS } from "../src/table-names";
import { minutes } from "./utils";

describe("Queries Across Tables", () => {
  let db: Database;

  beforeAll(async () => {
    db = await Database.fromExisting("06", "07");
  }, minutes(3));

  it(
    "should select top three directors ordered by total budget spent in their movies",
    async done => {
      const query = `Select full_name as director, round(sum(budget_adjusted),2) as total_budget FROM ${DIRECTORS}
      join ${MOVIE_DIRECTORS} on ${DIRECTORS}.id = ${MOVIE_DIRECTORS}.director_id
      join ${MOVIES} ON ${MOVIE_DIRECTORS}.movie_id = MOVIES.id
      group by director order by total_budget desc limit 3`;
      
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          director: "Ridley Scott",
          total_budget: 722882143.58
        },
        {
          director: "Michael Bay",
          total_budget: 518297522.1
        },
        {
          director: "David Yates",
          total_budget: 504100108.5
        }
      ]);

      done();
    },
    minutes(3)
  );

  it(
    "should select top 10 keywords ordered by their appearance in movies",
    async done => {
      const query = `Select keyword, count(keyword) as count from ${MOVIE_KEYWORDS}
      inner join ${KEYWORDS} on ${MOVIE_KEYWORDS}.keyword_id = ${KEYWORDS}.id
      group by keyword order by count desc limit 10`;

      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          keyword: "woman director",
          count: 162
        },
        {
          keyword: "independent film",
          count: 115
        },
        {
          keyword: "based on novel",
          count: 85
        },
        {
          keyword: "duringcreditsstinger",
          count: 82
        },
        {
          keyword: "biography",
          count: 78
        },
        {
          keyword: "murder",
          count: 66
        },
        {
          keyword: "sex",
          count: 60
        },
        {
          keyword: "revenge",
          count: 51
        },
        {
          keyword: "sport",
          count: 50
        },
        {
          keyword: "high school",
          count: 48
        }
      ]);

      done();
    },
    minutes(3)
  );

  it(
    "should select all movies called Life and return amount of actors",
    async done => {
      const query = `Select original_title as "original_title", count(full_name) as "count" FROM ${ACTORS}
      join ${MOVIE_ACTORS} on ${ACTORS}.id = ${MOVIE_ACTORS}.actor_id
      join ${MOVIES} ON ${MOVIE_ACTORS}.movie_id = ${MOVIES}.id
      where original_title = "Life"
      group by original_title order by full_name desc`;

      const result = await db.selectSingleRow(query);

      expect(result).toEqual({
        original_title: "Life",
        count: 12
      });

      done();
    },
    minutes(3)
  );

  it(
    "should select three genres which has most ratings with 5 stars",
    async done => {
      const query = `Select genre, count(*) as five_stars_count FROM ${GENRES}
      join ${MOVIE_GENRES} on ${MOVIE_GENRES}.genre_id = genres.id
      join ${MOVIE_RATINGS} on ${MOVIE_RATINGS}.movie_id = ${MOVIE_GENRES}.movie_id
      where ${MOVIE_RATINGS}.rating = 5
      group by genre order by five_stars_count desc limit 3`;

      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          genre: "Drama",
          five_stars_count: 15052
        },
        {
          genre: "Thriller",
          five_stars_count: 11771
        },
        {
          genre: "Crime",
          five_stars_count: 8670
        }
      ]);

      done();
    },
    minutes(3)
  );

  it(
    "should select top three genres ordered by average rating",
    async done => {
      const query = `Select genre, round(avg(rating),2) as avg_rating FROM ${GENRES}
      join ${MOVIE_GENRES} on ${MOVIE_GENRES}.genre_id = ${GENRES}.id
      join ${MOVIE_RATINGS} on ${MOVIE_RATINGS}.movie_id = ${MOVIE_GENRES}.movie_id   
      group by genre order by avg_rating desc limit 3`;
      
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          genre: "Crime",
          avg_rating: 3.79
        },
        {
          genre: "Music",
          avg_rating: 3.73
        },
        {
          genre: "Documentary",
          avg_rating: 3.71
        }
      ]);

      done();
    },
    minutes(3)
  );
});
