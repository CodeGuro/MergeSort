#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <mpi.h>

// one of the few cases where #defines are OK
#define TINY_SIZE 128
#define BAD_SIZE -1

// sorting algorithm of complexity O( nlogn )
int * mergesort( int *arr, int n )
{
	if( n < 2 )
		return arr;

	if( n == 2 )
	{
		if( arr[ 0 ] > arr[ 1 ] )
		{
			int tmp = arr[ 0 ];
			arr[ 0 ] = arr[ 1 ];
			arr[ 1 ] = tmp;
		}
		return arr;
	}

	int k = n / 2;
	int is_odd = n & 0x01;

	int *buff = malloc( n * sizeof( int ) );

	int *fhalf = mergesort( arr, k + is_odd );
	int *shalf = mergesort( arr + k + is_odd, k );

	int *fhalf_end = shalf;
	int *shalf_end = fhalf + n;
	int *ptr = buff;

	while( fhalf < fhalf_end && shalf < shalf_end )
	{
		if( *fhalf < *shalf )
			*ptr++ = *fhalf++;
		else
			*ptr++ = *shalf++;
	}

	while( shalf < shalf_end )
		*ptr++ = *shalf++;
	while( fhalf < fhalf_end )
		*ptr++ = *fhalf++;

	for( int it = 0; it < n; ++it )
		arr[ it ] = buff[ it ];

	free( buff );

	return arr;
}

// sorting algorithm of complexity O( n )
// this function assumes the both buffers of are sorted
void quick_merge( int *out, int *b1, int *b2, int n )
{
	int *b1_end = b1 + n;
	int *b2_end = b2 + n;

	while( b1 < b1_end && b2 < b2_end )
	{
		if( *b1 < *b2 )
			*out++ = *b1++;
		else
			*out++ = *b2++;
	}

	while( b1 < b1_end )
		*out++ = *b1++;
	while( b2 < b2_end )
		*out++ = *b2++;
}

// this function writes the output to the specified filename
void write_outfile( char const *fname, int *buff, int n )
{
	FILE *hfile = fopen( fname, "w" );

	fprintf( hfile, "Sorted array of %i elements:", n );

	for( int it = 0; it < n; ++it )
	{
		const char *delim = it % 10 ? " " : "\n";
		fprintf( hfile, "%s%i", delim, buff[ it ] );
	}

	fclose( hfile );
}

int main( int argc, char **argv )
{
	// initialize MPI
	MPI_Init( &argc, &argv );

	// get MPI info
	int rank;
	int comm_size;
	MPI_Comm_rank( MPI_COMM_WORLD, &rank );
	MPI_Comm_size( MPI_COMM_WORLD, &comm_size );

	// declarations
	printf( "Entering proc rank is %i\n", rank );
	int arrsize = -1;
	int *rec_buff = NULL;
	int rec_buff_size = -1;

	if( rank == 0 ) // parent
	{
		FILE *hfile = fopen( argv[ 1 ], "rb" );

		if( hfile == NULL ) // bad file
		{
			int placeholder = BAD_SIZE;
			MPI_Bcast( &placeholder, 1, MPI_INT, 0, MPI_COMM_WORLD );
			MPI_Finalize();
			return printf( "no such file %s\n", argv[1] );
		}

		// read the file for element count and cap the per-proc buffer size
		char buffer[ TINY_SIZE ] = { 0 };
		fgets( buffer, sizeof( buffer ), hfile );
		arrsize = atoi( buffer );
		rec_buff_size = ( arrsize / comm_size ) + ( ( arrsize % comm_size ) ? 1 : 0 );

		// map the buffer with the integers
		int *buff = malloc( rec_buff_size * comm_size * sizeof( int ) );
		for( int it = 0; it < arrsize; ++it )
		{
			memset( buffer, '\0', sizeof( buffer ) );
			char c;

			while( isspace( ( c = fgetc( hfile ) ) ) ); // skip the spaces

			for( int it2 = 0; it2 < sizeof( buffer ) && !isspace( c ); c = fgetc( hfile ) )
				buffer[ it2++ ] = c;

			buff[ it ] = atoi( buffer );
		}

		// fill the cap with MAX_INT (to be ignored later)
		for( int it = arrsize; it < rec_buff_size * comm_size; ++it )
			buff[ it ] = INT_MAX;
		
		// scatter the array across other processes 
		rec_buff = malloc( rec_buff_size * sizeof( int ) );
		MPI_Bcast( &rec_buff_size, 1, MPI_INT, 0, MPI_COMM_WORLD ); // broadcast the data size
		MPI_Scatter( buff, rec_buff_size, MPI_INT, rec_buff, rec_buff_size, MPI_INT, 0, MPI_COMM_WORLD );
		printf( "parent broadcast sent\n" );
		printf( "child rank %i recieved the buffer size. It is %i\n", rank, rec_buff_size );

		// clean up 
		free( buff );
		fclose( hfile );
	}
	else // child
	{
		// recieve the data size
		printf( "Entering child rank %i\n", rank );
		MPI_Bcast( &rec_buff_size, 1, MPI_INT, 0, MPI_COMM_WORLD );

		if( rec_buff_size == BAD_SIZE ) // invalid input filename
		{
			MPI_Finalize();
			return printf( "child rank %i recieved the terminate message\n", rank );
		}

		printf( "child rank %i recieved the buffer size. It is %i\n", rank, rec_buff_size );
		rec_buff = malloc( rec_buff_size * sizeof( int ) );

		// recieve the data
		MPI_Scatter( NULL, 0, MPI_DATATYPE_NULL, rec_buff, rec_buff_size, MPI_INT, 0, MPI_COMM_WORLD );
		printf( "child rank %i recieved the data\n", rank );
	}

	// finally, sort the data
	mergesort( rec_buff, rec_buff_size );

	// begin merging the data
	for( int step = 1; step < comm_size; step *= 2 )
	{
		if( rank % (2 * step) )
		{
			MPI_Send( rec_buff, rec_buff_size, MPI_INT, rank - step, 0, MPI_COMM_WORLD ); // send data for merging
			break; // we are now finished for this proc
		}
		else
		{
			if( rank + step < comm_size )
			{
				// allocate a second buffer to recieve data, and double the buffer size
				printf( "rank %i is merging from rank %i\n", rank, rank + step );
				int *second_buff = malloc( rec_buff_size * sizeof( int ) );
				int *new_rec_buff = malloc( rec_buff_size * 2 * sizeof( int ) );
				MPI_Recv( second_buff, rec_buff_size, MPI_INT, rank + step, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
				quick_merge( new_rec_buff, rec_buff, second_buff, rec_buff_size );
				free( second_buff );
				free( rec_buff );
				rec_buff = new_rec_buff;
				rec_buff_size *= 2;
			}
		}
	}

	// create and write the output file
	if( rank == 0 )
	{
		char *outfilename = argc > 2 ? argv[ 2 ] : "outfile";
		write_outfile( argv[ 2 ], rec_buff, arrsize );
	}

	// clean up
	free( rec_buff );
	MPI_Finalize();

	return 0;
}