import { ACTORS, DIRECTORS, GENRES, KEYWORDS, MOVIES, MOVIE_RATINGS, PRODUCTION_COMPANIES } from "../table-names";

export const selectActorByName = (fullName: string): string => {
  return `SELECT * from ${ACTORS} WHERE full_name = '${fullName}'`;
};

export const selectKeyword = (keyword: string): string => {
  return `SELECT * from ${KEYWORDS} WHERE keyword = '${keyword}'`
};

export const selectDirector = (director: string): string => {
  return `SELECT * from ${DIRECTORS} WHERE full_Name = '${director}'`
};

export const selectGenre = (genre: string): string => {
  return `SELECT * from ${GENRES} WHERE genre = '${genre}'`
};

export const selectProductionCompany = (company: string): string => {
  return `SELECT * from ${PRODUCTION_COMPANIES} WHERE company_name = '${company}'`
};

export const selectMovieById = (id: number): string => {
  return `SELECT * from ${MOVIES} WHERE id = ${id}`
};

export const selectGenreById = (id: number): string => {
  return `SELECT * from ${GENRES} WHERE id = ${id}`
};

export const selectDirectorById = (id: number): string => {
  return `SELECT * from ${DIRECTORS} WHERE id = ${id}`
};

export const selectActorById = (id: number): string => {
  return `SELECT * from ${ACTORS} WHERE id = ${id}`
};

export const selectKeywordById = (id: number): string => {
  return `SELECT * from ${KEYWORDS} WHERE id = ${id}`
};

export const selectProductionCompanyById = (id: number): string => {
  return `SELECT * from ${PRODUCTION_COMPANIES} WHERE id = ${id}`
};

export const selectMovie = (imdbId: string): string => {
  return `SELECT * from ${MOVIES} WHERE imdb_id = '${imdbId}'`
};

export const selectMovieId = (imdbId: string): string => {
  return `SELECT id from ${MOVIES} WHERE imdb_id = '${imdbId}'`
};

export const selectRatingsByUserID = (userId: number): string => {
  return `SELECT * from ${MOVIE_RATINGS} WHERE user_Id  = ${userId}`
};

export const selectGenresByMovieId = (movieId: number): string => {
  return `select g.genre from movie_genres mg join genres g on g.id = mg.genre_id where mg.movie_id = ${movieId}`;
};

export const selectActorsByMovieId = (movieId: number): string => {
  return `select a.full_name from movie_actors ma join actors a on a.id = ma.actor_id where ma.movie_id = ${movieId}`;
};

export const selectDirectorsByMovieId = (movieId: number): string => {
  return `select d.full_name from movie_directors md join directors d on d.id = md.director_id where md.movie_id = ${movieId}`;
};

export const selectKeywordsByMovieId = (movieId: number): string => {
  return `select k.keyword from movie_keywords mk join keywords k on k.id = mk.keyword_id where mk.movie_id = ${movieId}`;
};

export const selectProductionCompaniesByMovieId = (movieId: number): string => {
  return `select pc.company_name from movie_production_companies mpc join production_companies pc on pc.id = mpc.company_id where mpc.movie_id = ${movieId}`;
};

export const selectCount = (table: string): string => {
  return `SELECT count (*) as c from ${table}`
};
