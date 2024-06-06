using BusComun;
using Comun;
using System;
using System.Data;


namespace BusInchost
{
    public class TyTelefono
    {
        public string TECOD;
        public string TEORIGEN;
        public string TECODTEL;
        public string TETIPTEL;
        public string TENUMERO;
        public int TEPRIORIDAD;
        public int TEUSRMODIF;
        public string TEDEFAULT;
        public string TEOBS;
        public string TEFECBAJA;
        public string TECODPAIS;
        public string TECODAREA;
        public string TEVALIDO;
    }

    class TelefonosDalc : cBase
    {
        public TelefonosDalc(cConexion pconn)
        {
            conn = pconn;
        }

        #region Proceso Unitario

        internal AISDataReader AbrirCursorNuevosTelefonos()
        {
            AISDataReader Dr = new AISDataReader();

            string sSql = $@" SELECT substr(TECOD, 1,3) as TEORIGEN, TECOD, TETIPTEL, substr(TECODTEL, 4, length(TECODTEL)-1) as TECODTEL, 
TECODPAIS, TECODAREA, TENUMERO, TEDEFAULT, TEOBS, TEFECBAJA
            FROM IN_TELE  
            WHERE TEFECPROC = {cFormat.StToBD(cGlobales.Hoy)} AND TEFECBAJA is null
            ORDER BY TECOD  ";

            Dr = conn.EjecutarDataReader(sSql);

            Errores = conn.Errores;
            return Dr;
        }

        internal bool FechTelefono(AISDataReader Dr, ref TyTelefono Telefono)
        {
            try
            {
                Telefono = new TyTelefono();
                if (Dr.Read())
                {
                    Telefono.TECOD = Dr["TECOD"].ToString();
                    Telefono.TEORIGEN = Dr["TEORIGEN"].ToString();
                    Telefono.TECODTEL = Dr["TECODTEL"].ToString();
                    Telefono.TETIPTEL = Dr["TETIPTEL"].ToString();
                    Telefono.TENUMERO = Dr["TENUMERO"].ToString();
                    Telefono.TEPRIORIDAD = Dr["TEDEFAULT"].ToString() == "S" ? 100 : 10;
                    Telefono.TEDEFAULT = Dr["TEDEFAULT"].ToString();
                    Telefono.TEOBS = Dr["TEOBS"].ToString();
                    Telefono.TEFECBAJA = Dr["TEFECBAJA"].ToString();
                    Telefono.TECODPAIS = Dr["TECODPAIS"].ToString();
                    Telefono.TECODAREA = Dr["TECODAREA"].ToString();
                    Telefono.TEVALIDO = "1";

                    return true;
                }
                else
                    return false;

            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechTelefono", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHTELE", "Error al recuperar Telefono");
                return false;
            }
        }

        internal TipoAccion ObtengoTelefonoBD(TyTelefono BDTelefono)
        {
            DataSet Ds = new DataSet();

            string sSql = $@" SELECT TECOD, TEORIGEN, TECODTEL, TETIPTEL, TENUMERO, TEPRIORIDAD, TEUSRMODIF, TEDEFAULT, TEOBS,
    TEFECBAJA, TECODPAIS, TECODAREA, TEVALIDO 
 FROM RTELE  
 WHERE TECOD = {cFormat.StToBD(BDTelefono.TECOD)} 
    AND TEORIGEN ={cFormat.StToBD(BDTelefono.TEORIGEN)} 
    AND TECODTEL = {cFormat.StToBD(BDTelefono.TECODTEL)}
    AND TETIPTEL = {cFormat.StToBD(BDTelefono.TETIPTEL)} ";

            Ds = conn.EjecutarQuery(sSql, "RTELE");

            if (conn.Errores.Cantidad() == 0)
            {
                if (Ds.Tables["RTELE"].Rows.Count > 0)
                {
                    DataRow Dr = Ds.Tables["RTELE"].Rows[0];

                    BDTelefono.TECOD = Dr["TECOD"].ToString();
                    BDTelefono.TEORIGEN = Dr["TEORIGEN"].ToString();
                    BDTelefono.TECODTEL = Dr["TECODTEL"].ToString();
                    BDTelefono.TETIPTEL = Dr["TETIPTEL"].ToString();
                    BDTelefono.TENUMERO = Dr["TENUMERO"].ToString();
                    BDTelefono.TEPRIORIDAD = int.Parse(Dr["TEPRIORIDAD"].ToString());
                    BDTelefono.TEUSRMODIF = int.Parse(Dr["TEUSRMODIF"].ToString());
                    BDTelefono.TEDEFAULT = Dr["TEDEFAULT"].ToString();
                    BDTelefono.TEOBS = Dr["TEOBS"].ToString();
                    BDTelefono.TEFECBAJA = Dr["TEFECBAJA"].ToString();
                    BDTelefono.TECODPAIS = Dr["TECODPAIS"].ToString();
                    BDTelefono.TECODAREA = Dr["TECODAREA"].ToString();
                    BDTelefono.TEVALIDO = Dr["TEVALIDO"].ToString();

                    return TipoAccion.Modificacion;
                }
                else
                    return TipoAccion.Alta;
            }
            else
            {
                cIncidencia.Generar(conn.Errores, "SELTELE", "Error en Select de Telefonos. Cliente: " + BDTelefono.TECOD + " - Codigo Telefono: " + BDTelefono.TECODTEL + " - Tipo Telefono: " + BDTelefono.TETIPTEL);
                return TipoAccion.Error;
            }
        }

        internal bool InsertaTelefonoBD(TyTelefono Telefono)
        {
            int nRet;

            string sSql = $@" INSERT INTO RTELE ( TECOD, TEORIGEN, TECODTEL, TETIPTEL, TENUMERO, TEPRIORIDAD, TEDEFAULT, TEOBS, 
TEFECBAJA, TECODPAIS, TECODAREA, TEVALIDO) VALUES ( 
{cFormat.StToBD(Telefono.TECOD)}, {cFormat.StToBD(Telefono.TEORIGEN)}, {cFormat.StToBD(Telefono.TECODTEL)}, {cFormat.StToBD(Telefono.TETIPTEL)},
{cFormat.StToBD(Telefono.TENUMERO)}, {cFormat.NumToBD(Telefono.TEPRIORIDAD.ToString())}, 
{cFormat.StToBD(Telefono.TEDEFAULT)}, {cFormat.StToBD(Telefono.TEOBS)}, {cFormat.StToBD(Telefono.TEFECBAJA)}, {cFormat.StToBD(Telefono.TECODPAIS)},
{cFormat.StToBD(Telefono.TECODAREA)}, {cFormat.StToBD(Telefono.TEVALIDO)} )";

            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertaTelefonoNewBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSTELE", "Error al insertar telefono. Cliente: " + Telefono.TECOD + " - Codigo Telefono: " + Telefono.TECODTEL + " - Tipo Telefono: " + Telefono.TETIPTEL);
                return false;
            }
        }

        internal bool ModificaTelefonoBD(TyTelefono Telefono)
        {
            int nRet;

            string sSql = $@" UPDATE RTELE SET  
    TENUMERO = {cFormat.StToBD(Telefono.TENUMERO)}, 
    TEPRIORIDAD = {cFormat.NumToBD(Telefono.TEPRIORIDAD.ToString())}, 
    TEDEFAULT = {cFormat.StToBD(Telefono.TEDEFAULT)}, 
    TEOBS = {cFormat.StToBD(Telefono.TEOBS)}, 
    TEFECBAJA = {cFormat.StToBD(Telefono.TEFECBAJA)}, 
    TECODPAIS = {cFormat.StToBD(Telefono.TECODPAIS)}, 
    TECODAREA = {cFormat.StToBD(Telefono.TEVALIDO)}, 
    TEVALIDO = {cFormat.StToBD(Telefono.TECOD)}
WHERE TECOD = {cFormat.StToBD(Telefono.TECOD)} AND 
      TEORIGEN = {cFormat.StToBD(Telefono.TEORIGEN)} AND 
      TECODTEL = {cFormat.StToBD(Telefono.TECODTEL)} AND  
      TETIPTEL = {cFormat.StToBD(Telefono.TETIPTEL)} ";

            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaTelefonoNEWBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDTELE", "Error al modificar telefono. Cliente: " + Telefono.TECOD + " - Codigo Telefono: " + Telefono.TECODTEL + " - Tipo Telefono: " + Telefono.TETIPTEL);
                return false;
            }
        }

        #endregion

        #region Proceso Masivo

        /// <summary>
        /// Borrado masivo de telefonos productivos que existan en la IN
        /// </summary>
        internal void BorradoTelefonos()
        {
            string sSql = $@" DELETE FROM RTELE P 
WHERE P.TEORIGEN IN ('COM', 'LSG') 
    AND EXISTS (SELECT 1 FROM IN_TELE I WHERE I.TEFECPROC = {cFormat.StToBD(cGlobales.Hoy)} 
                AND P.TECOD = I.TECOD 
                AND P.TETIPTEL = I.TETIPTEL) ";

            conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaGarantiaBD", Const.SEVERIDAD_Alta);
            }
        }

        /// <summary>
        /// Insert masivo de teléfono
        /// </summary>
        internal void InsertTelefonos()
        {
            string sSql = $@"insert into RTELE (TECOD, TETIPTEL, TECODTEL, TECODPAIS, TECODAREA, TENUMERO, TEDEFAULT, TEOBS, TEORIGEN)
SELECT  TECOD, TETIPTEL, TECODTEL, TECODPAIS, TECODAREA, TENUMERO, TEDEFAULT, TEOBS, SUBSTR(TECOD, 1, 3) TEORIGEN
FROM IN_TELE 
WHERE TEFECPROC = {cFormat.StToBD(cGlobales.Hoy)} AND EXISTS (SELECT 1 FROM RCLIE WHERE CLCOD = TECOD) ";

            int res = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaGarantiaBD", Const.SEVERIDAD_Alta);
            }
            else
            {
                cIncidencia.Aviso($"Telefonos tratados: {res}");
            }
        }

        #endregion
    }
}
